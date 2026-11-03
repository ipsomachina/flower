//! The derived graph an evaluation read through, so a cached result outlives
//! a change to a derived value's inputs that leaves the value as it was:
//! unchanged-output propagation, at read time.
//!
//! [`Certificate::observe`] flattens derived reads into the records and
//! ranges their outcomes read, so any change to those touches the
//! result, even when every derived value it read would come out the
//! same. A [`Graph`] keeps what the flattening loses: what the
//! evaluation read itself (its body), and for each derived instance it
//! read, directly or through others, what that instance read itself,
//! which instances it read, and a digest of the outcome it served.
//!
//! A reader whose cached result a change touched only through derived
//! instances re-derives the instances whose own reads the change touched,
//! children first, and an instance above them only when one of its children
//! came out different. When every instance the body read comes out as it was,
//! the body, reading the same values and nothing the change touched, answers
//! what it answered: the cached result stands, with the graph of the new
//! derivations ([`Graph::refreshed`]). Instances the change left alone keep
//! their part of the graph, though their committed outcomes may have dropped
//! with the change: they read nothing it touched and only children that came
//! out the same.
//!
//! Only stable results keep a graph worth checking: one that read the
//! clock, a managed key or something no dependency names never enters
//! the cache, and the reader re-evaluates one that declared an instant.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use sha2::{Digest as _, Sha256};

use super::{Certificate, ChangeSet, MAX_DEPENDENCIES, Validity};
use crate::error::Result;
use crate::session::Staged;
use crate::storage::ReadTxn;
use crate::storage::codec::{DerivedOutcome, OutcomeBody, decode_outcome};
use crate::storage::keys::encode_derived_key;

/// A derived instance: its definition and canonical arguments.
pub type Instance = (u32, Vec<u8>);

/// A SHA-256 digest of an outcome as served.
pub type Digest = [u8; 32];

/// The edges an outcome's evaluation recorded, and its digest.
type Current = (Vec<Vec<u8>>, Digest);

/// One derived instance an evaluation read.
#[derive(Clone, Debug)]
struct Node {
    instance: Instance,
    /// What its evaluation read besides derived instances.
    direct: Certificate,
    /// The instances it read, as indexes of earlier nodes.
    children: Vec<usize>,
    /// The digest of the outcome it served.
    digest: [u8; 32],
}

/// What an evaluation read, as the derived graph it read through.
#[derive(Clone, Debug)]
pub struct Graph {
    /// What the evaluation read besides derived instances.
    body: Certificate,
    /// Every instance it read, directly or not, children first.
    nodes: Vec<Node>,
    /// The instances it read itself.
    roots: Vec<usize>,
    /// Approximate retained bytes.
    bytes: usize,
}

/// The digest of an outcome as served: a value or a business failure.
fn digest(outcome: &DerivedOutcome) -> Option<[u8; 32]> {
    let (tag, body): (u8, &[u8]) = match &outcome.body {
        OutcomeBody::Value(json) => (0, json),
        OutcomeBody::Error(json) => (1, json),
        OutcomeBody::Invalid => return None,
    };
    let mut hasher = Sha256::new();
    hasher.update([tag]);
    hasher.update(body);
    Some(hasher.finalize().into())
}

/// The outcome `instance` serves in `read` overlaid with `staged`, with
/// the edges its evaluation recorded; `None` when it serves none.
fn current(
    read: &ReadTxn,
    staged: &Staged,
    db: &[u8],
    (definition, args): &Instance,
) -> Result<Option<Current>> {
    let key = encode_derived_key(db, *definition, args)?;
    if staged.derived_dropped(&key) {
        return Ok(None);
    }
    let (outcome, edges) = match staged.staged_derived(&key) {
        Some(bytes) => {
            let edges = match staged.staged_observed(&key) {
                Some(edges) => edges.to_vec(),
                None => read.dependent_edges(db, *definition, args)?,
            };
            (decode_outcome(bytes)?, edges)
        }
        None => match read.get_derived(db, *definition, args)? {
            Some(outcome) => (outcome, read.dependent_edges(db, *definition, args)?),
            None => return Ok(None),
        },
    };
    Ok(digest(&outcome).map(|digest| (edges, digest)))
}

/// The digest of the outcome `instance` serves in `read` overlaid with
/// `staged`; `None` when it serves none.
pub(crate) fn served(
    read: &ReadTxn,
    staged: &Staged,
    db: &[u8],
    instance: &Instance,
) -> Result<Option<[u8; 32]>> {
    Ok(current(read, staged, db, instance)?.map(|(_, digest)| digest))
}

/// Builds a graph from the current state, keeping the nodes of an
/// earlier one where asked.
struct Builder<'a> {
    read: &'a ReadTxn,
    staged: &'a Staged,
    db: &'a [u8],
    /// An earlier graph, and which of its nodes to observe again.
    earlier: Option<(&'a Graph, &'a [bool], HashMap<&'a Instance, usize>)>,
    nodes: Vec<Node>,
    index: HashMap<Instance, usize>,
    visiting: HashSet<Instance>,
    dependencies: usize,
}

impl Builder<'_> {
    /// The node of `instance`, built once; `None` when the graph cannot
    /// be told (an instance serving no outcome, a cycle, too large).
    fn node(&mut self, instance: Instance) -> Result<Option<usize>> {
        if let Some(index) = self.index.get(&instance) {
            return Ok(Some(*index));
        }
        if !self.visiting.insert(instance.clone()) {
            return Ok(None);
        }
        let kept = self.earlier.as_ref().and_then(|(graph, observe, index)| {
            let at = *index.get(&instance)?;
            let node = &graph.nodes[at];
            (!observe[at]).then(|| {
                let children: Vec<Instance> = node
                    .children
                    .iter()
                    .map(|child| graph.nodes[*child].instance.clone())
                    .collect();
                (node.direct.clone(), children, node.digest)
            })
        });
        let (direct, children, digest) = match kept {
            Some(kept) => kept,
            None => {
                let Some((edges, digest)) = current(self.read, self.staged, self.db, &instance)?
                else {
                    return Ok(None);
                };
                let (direct, children) = Certificate::direct(&edges)?;
                (direct, children, digest)
            }
        };
        self.dependencies += direct.dependencies();
        if self.dependencies > MAX_DEPENDENCIES {
            return Ok(None);
        }
        let mut indexes = Vec::with_capacity(children.len());
        for child in children {
            match self.node(child)? {
                Some(index) => indexes.push(index),
                None => return Ok(None),
            }
        }
        self.visiting.remove(&instance);
        let index = self.nodes.len();
        self.index.insert(instance.clone(), index);
        self.nodes.push(Node {
            instance,
            direct,
            children: indexes,
            digest,
        });
        Ok(Some(index))
    }

    /// The graph of `body` reading `roots`.
    fn finish(mut self, body: Certificate, roots: Vec<Instance>) -> Result<Option<Graph>> {
        let mut indexes = Vec::with_capacity(roots.len());
        for root in roots {
            match self.node(root)? {
                Some(index) => indexes.push(index),
                None => return Ok(None),
            }
        }
        let bytes = body.allocation_cost()
            + self
                .nodes
                .iter()
                .map(|node| {
                    node.direct.allocation_cost()
                        + node.instance.1.len()
                        + 8 * node.children.len()
                        + 64
                })
                .sum::<usize>();
        Ok(Some(Graph {
            body,
            nodes: self.nodes,
            roots: indexes,
            bytes,
        }))
    }
}

impl Graph {
    /// The graph of an evaluation that recorded `deps`, from the derived
    /// outcomes staged in `staged`, else committed in `read`; `None`
    /// when it read no derived instance, or the graph cannot be told.
    pub fn observe(
        read: &ReadTxn,
        staged: &Staged,
        db: &[u8],
        deps: &[Vec<u8>],
    ) -> Result<Option<Graph>> {
        let (body, roots) = Certificate::direct(deps)?;
        if roots.is_empty() {
            return Ok(None);
        }
        let builder = Builder {
            read,
            staged,
            db,
            earlier: None,
            nodes: Vec::new(),
            index: HashMap::new(),
            visiting: HashSet::new(),
            dependencies: body.dependencies(),
        };
        builder.finish(body, roots)
    }

    /// This graph once the nodes marked `rederived` were derived again
    /// over `read` and `staged` and every instance the body read came
    /// out as it was: those nodes take what their new derivation read,
    /// the others stay as they were.
    pub fn refreshed(
        &self,
        read: &ReadTxn,
        staged: &Staged,
        db: &[u8],
        rederived: &[bool],
    ) -> Result<Option<Graph>> {
        let index = self
            .nodes
            .iter()
            .enumerate()
            .map(|(at, node)| (&node.instance, at))
            .collect();
        let builder = Builder {
            read,
            staged,
            db,
            earlier: Some((self, rederived, index)),
            nodes: Vec::new(),
            index: HashMap::new(),
            visiting: HashSet::new(),
            dependencies: self.body.dependencies(),
        };
        let roots = self
            .roots
            .iter()
            .map(|root| self.nodes[*root].instance.clone())
            .collect();
        builder.finish(self.body.clone(), roots)
    }

    /// Which nodes `changes` touch through what they read themselves;
    /// `None` when they touch what the body read itself.
    pub fn touched(&self, changes: &[Arc<ChangeSet>]) -> Option<Vec<bool>> {
        if changes.iter().any(|changes| self.body.touched_by(changes)) {
            return None;
        }
        Some(
            self.nodes
                .iter()
                .map(|node| {
                    changes
                        .iter()
                        .any(|changes| node.direct.touched_by(changes))
                })
                .collect(),
        )
    }

    /// The nodes, children first: each one's instance, the indexes of
    /// its children and its outcome's digest.
    pub fn nodes(&self) -> impl Iterator<Item = (&Instance, &[usize], &[u8; 32])> {
        self.nodes
            .iter()
            .map(|node| (&node.instance, node.children.as_slice(), &node.digest))
    }

    /// The indexes of the instances the body read itself.
    pub fn roots(&self) -> &[usize] {
        &self.roots
    }

    /// Everything it read, flattened as [`Certificate::observe`] does;
    /// broad past [`MAX_DEPENDENCIES`].
    pub fn certificate(&self) -> Certificate {
        let mut certificate = self.body.clone();
        for node in &self.nodes {
            certificate.merge(&node.direct);
        }
        if certificate.dependencies() > MAX_DEPENDENCIES {
            return Certificate::broad();
        }
        certificate
    }

    /// Whether every part is stable: it read no clock, managed key or
    /// untracked state.
    pub fn is_stable(&self) -> bool {
        std::iter::once(&self.body)
            .chain(self.nodes.iter().map(|node| &node.direct))
            .all(|part| {
                !part.is_broad() && !part.reads_keys() && part.validity() == Validity::Stable
            })
    }

    /// Approximate retained bytes.
    pub fn allocation_cost(&self) -> usize {
        self.bytes
    }
}
