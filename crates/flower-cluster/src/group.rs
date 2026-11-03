//! Group identity: which Raft group a replica, a connection or a
//! request belongs to.
//!
//! One process hosts any number of groups (see [`crate::host`]). A group
//! has a name, which addresses it, and a random non-zero *uid* the
//! catalog fixes when it creates the group, which tells apart two groups
//! that held one name at different times. The bootstrap group is
//! [`ROOT`], uid 0: the one group of every single-group deployment,
//! whose files stay where they always were.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// The bootstrap group's name.
pub const ROOT: &str = "root";
/// Longest group name, in bytes.
pub const MAX_NAME: usize = 63;

/// A group's identity: its name and the uid the catalog gave it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GroupId {
    /// The group's name (see [`validate_name`]).
    pub name: Arc<str>,
    /// Its uid: zero for [`ROOT`], random and non-zero for the others.
    pub uid: u64,
}

impl GroupId {
    /// The bootstrap group.
    pub fn root() -> Self {
        GroupId {
            name: ROOT.into(),
            uid: 0,
        }
    }

    /// A group the catalog created, after checking its name.
    pub fn new(name: &str, uid: u64) -> Result<Self, String> {
        validate_name(name)?;
        if (name == ROOT) != (uid == 0) {
            return Err(format!(
                "group `{name}` cannot have uid {uid}: only `{ROOT}` has uid 0"
            ));
        }
        Ok(GroupId {
            name: name.into(),
            uid,
        })
    }

    /// Whether this is the bootstrap group.
    pub fn is_root(&self) -> bool {
        &*self.name == ROOT
    }
}

impl Default for GroupId {
    fn default() -> Self {
        GroupId::root()
    }
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_root() {
            f.write_str(ROOT)
        } else {
            write!(f, "{}#{:016x}", self.name, self.uid)
        }
    }
}

/// Check a group name: 1 to 63 bytes of lowercase ASCII letters, digits
/// and hyphens, starting with a letter or digit. Names are path
/// components of the data directory and travel in hellos.
pub fn validate_name(name: &str) -> Result<(), String> {
    let valid = !name.is_empty()
        && name.len() <= MAX_NAME
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !name.starts_with('-');
    if valid {
        Ok(())
    } else {
        Err(format!(
            "group name `{name}` must be 1 to {MAX_NAME} lowercase letters, digits or hyphens, \
             not starting with a hyphen"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_path_safe_and_only_root_has_uid_zero() {
        for name in ["root", "east", "g-1", "0", &"a".repeat(MAX_NAME)] {
            validate_name(name).unwrap();
        }
        for name in [
            "",
            "East",
            "-a",
            "a_b",
            "a/b",
            "..",
            "a.b",
            &"a".repeat(MAX_NAME + 1),
        ] {
            assert!(validate_name(name).is_err(), "{name:?}");
        }
        assert!(GroupId::new("east", 7).is_ok());
        assert!(GroupId::new("east", 0).is_err());
        assert!(GroupId::new("root", 7).is_err());
        assert_eq!(GroupId::new("root", 0).unwrap(), GroupId::root());
        assert!(GroupId::root().is_root());
        assert_eq!(GroupId::root().to_string(), "root");
        assert_eq!(
            GroupId::new("east", 255).unwrap().to_string(),
            "east#00000000000000ff"
        );
    }
}
