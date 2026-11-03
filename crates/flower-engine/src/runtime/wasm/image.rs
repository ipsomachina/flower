//! Initialized images: a guest module rewritten so that instantiating it
//! yields the state its `flower_init` left behind.
//!
//! Preparation reads the module three times. [`inspect`] proves that the
//! one linear memory and the mutable globals are the module's whole
//! mutable state, so a snapshot of them is a faithful image: imports are
//! functions only, globals holding references and instructions that
//! change tables or segments are refused. [`instrument`] exports every
//! mutable global, hidden ones like the shadow-stack pointer included, so
//! the host can read them once `flower_init` returns. [`image`] writes the
//! original module back with the snapshot as its initial state: the
//! memory's minimum becomes the snapshot's size, its non-zero bytes
//! become the only data segments, mutable globals start at the values
//! they held, and the start function and the `flower_init` export go,
//! since what they did is already in the snapshot. Code, tables and
//! element segments stay byte for byte, and the image keeps the global
//! exports [`instrument`] added: Wasmtime compiles functions differently
//! once their module's globals are exported, so keeping them lets the
//! image reuse every function the snapshot run compiled.

use std::ops::Range;

use wasm_encoder::Encode;
use wasmparser::{Encoding, Operator, Parser, Payload, TypeRef, ValType};

/// Export name prefix [`instrument`] gives mutable globals.
const GLOBAL_EXPORT: &str = "__flower_image_global_";
/// Zero bytes between two non-zero runs that still join them into one
/// data segment.
const SEGMENT_GAP: usize = 4096;
/// Most data segments an image holds: denser snapshots join runs across
/// wider gaps until they fit.
const MAX_SEGMENTS: usize = 10_000;
/// Linear-memory page size.
const PAGE: usize = 64 * 1024;

/// The mutable state of a module [`inspect`] accepted.
#[derive(Debug)]
pub(super) struct Surface {
    /// Index and type of every mutable global, in index order.
    pub(super) globals: Vec<(u32, ValType)>,
}

/// What an instance of the instrumented module held after `flower_init`.
pub(super) struct Snapshot {
    /// Every byte of linear memory.
    pub(super) memory: Vec<u8>,
    /// The values of [`Surface::globals`], in the same order.
    pub(super) globals: Vec<wasmtime::Val>,
}

/// The export through which [`instrument`] exposes mutable global `index`.
pub(super) fn global_export(index: u32) -> String {
    format!("{GLOBAL_EXPORT}{index}")
}

/// Check that memory and mutable globals are the module's whole mutable
/// state and return them.
pub(super) fn inspect(wasm: &[u8]) -> Result<Surface, String> {
    let mut globals = Vec::new();
    let mut memories = 0;
    for payload in Parser::new(0).parse_all(wasm) {
        match payload.map_err(malformed)? {
            Payload::Version { encoding, .. } if encoding != Encoding::Module => {
                return Err("guest must be a core Wasm module".to_string());
            }
            Payload::ImportSection(section) => {
                for import in section.into_imports() {
                    let import = import.map_err(malformed)?;
                    if !matches!(import.ty, TypeRef::Func(_)) {
                        return Err(format!(
                            "guest imports `{}.{}`, which is not a function: an initialized image cannot hold imported state",
                            import.module, import.name
                        ));
                    }
                }
            }
            Payload::MemorySection(section) => {
                for memory in section {
                    let memory = memory.map_err(malformed)?;
                    if memory.memory64 || memory.shared || memory.page_size_log2.is_some() {
                        return Err(
                            "guest memory must be a private 32-bit memory with 64 KiB pages"
                                .to_string(),
                        );
                    }
                    memories += 1;
                }
            }
            Payload::GlobalSection(section) => {
                for (index, global) in (0u32..).zip(section) {
                    let global = global.map_err(malformed)?;
                    if !global.ty.mutable {
                        continue;
                    }
                    if global.ty.shared || !numeric(global.ty.content_type) {
                        return Err(format!(
                            "guest global {index} is shared or holds a reference: an initialized image cannot hold it"
                        ));
                    }
                    globals.push((index, global.ty.content_type));
                }
            }
            Payload::ExportSection(section) => {
                for export in section {
                    let export = export.map_err(malformed)?;
                    if export.name.starts_with(GLOBAL_EXPORT) {
                        return Err(format!(
                            "guest export `{}` uses a name the host reserves",
                            export.name
                        ));
                    }
                }
            }
            Payload::CodeSectionEntry(body) => {
                let mut operators = body.get_operators_reader().map_err(malformed)?;
                while !operators.eof() {
                    let operator = operators.read().map_err(malformed)?;
                    if changes_hidden_state(&operator) {
                        return Err(format!(
                            "guest uses `{operator:?}`, which changes table or segment state an initialized image cannot hold"
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    if memories != 1 {
        return Err("guest must define exactly one memory".to_string());
    }
    Ok(Surface { globals })
}

/// The module with every mutable global exported, for the snapshot run.
pub(super) fn instrument(wasm: &[u8], surface: &Surface) -> Result<Vec<u8>, String> {
    rewrite(wasm, surface, None)
}

/// The initialized image: the module with `snapshot` as its initial state.
pub(super) fn image(
    wasm: &[u8],
    surface: &Surface,
    snapshot: &Snapshot,
) -> Result<Vec<u8>, String> {
    if !snapshot.memory.len().is_multiple_of(PAGE)
        || snapshot.globals.len() != surface.globals.len()
    {
        return Err("image snapshot does not match the module".to_string());
    }
    rewrite(wasm, surface, Some(snapshot))
}

/// Copy `wasm` section by section: [`instrument`] without a snapshot,
/// [`image`] with one.
fn rewrite(wasm: &[u8], surface: &Surface, snapshot: Option<&Snapshot>) -> Result<Vec<u8>, String> {
    let mut out = wasm_encoder::Module::new();
    let mut data_written = false;
    for payload in Parser::new(0).parse_all(wasm) {
        match payload.map_err(malformed)? {
            Payload::Version { .. } | Payload::CodeSectionEntry(_) | Payload::End(_) => {}
            Payload::TypeSection(section) => copy(&mut out, 1, wasm, section.range())?,
            Payload::ImportSection(section) => copy(&mut out, 2, wasm, section.range())?,
            Payload::FunctionSection(section) => copy(&mut out, 3, wasm, section.range())?,
            Payload::TableSection(section) => copy(&mut out, 4, wasm, section.range())?,
            Payload::TagSection(section) => copy(&mut out, 13, wasm, section.range())?,
            Payload::ElementSection(section) => copy(&mut out, 9, wasm, section.range())?,
            Payload::CodeSectionStart { range, .. } => copy(&mut out, 10, wasm, range)?,
            Payload::CustomSection(section) => copy(&mut out, 0, wasm, section.range())?,
            Payload::MemorySection(section) => match snapshot {
                None => copy(&mut out, 5, wasm, section.range())?,
                Some(snapshot) => {
                    let mut memories = wasm_encoder::MemorySection::new();
                    for memory in section {
                        let memory = memory.map_err(malformed)?;
                        memories.memory(wasm_encoder::MemoryType {
                            minimum: (snapshot.memory.len() / PAGE) as u64,
                            maximum: memory.maximum,
                            memory64: false,
                            shared: false,
                            page_size_log2: None,
                        });
                    }
                    out.section(&memories);
                }
            },
            Payload::GlobalSection(section) => match snapshot {
                None => copy(&mut out, 6, wasm, section.range())?,
                Some(snapshot) => {
                    let end = section.range().end;
                    let items = section
                        .into_iter_with_offsets()
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(malformed)?;
                    let mut values = surface.globals.iter().zip(&snapshot.globals);
                    let mut globals = wasm_encoder::GlobalSection::new();
                    for (position, (offset, global)) in items.iter().enumerate() {
                        let next = items.get(position + 1).map_or(end, |(next, _)| *next);
                        if !global.ty.mutable {
                            globals.raw(slice(wasm, *offset..next)?);
                            continue;
                        }
                        let ((_, ty), value) = values
                            .next()
                            .ok_or_else(|| "image snapshot misses a global".to_string())?;
                        globals.global(
                            wasm_encoder::GlobalType {
                                val_type: encoder_type(*ty)?,
                                mutable: true,
                                shared: false,
                            },
                            &constant(value)?,
                        );
                    }
                    out.section(&globals);
                }
            },
            Payload::ExportSection(section) => {
                let end = section.range().end;
                let items = section
                    .into_iter_with_offsets()
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(malformed)?;
                let mut count = 0u32;
                let mut entries = Vec::new();
                for (position, (offset, export)) in items.iter().enumerate() {
                    // The image never initializes again.
                    if snapshot.is_some() && export.name == "flower_init" {
                        continue;
                    }
                    let next = items.get(position + 1).map_or(end, |(next, _)| *next);
                    entries.extend_from_slice(slice(wasm, *offset..next)?);
                    count += 1;
                }
                // Host-reserved names: only the host reads them.
                for (index, _) in &surface.globals {
                    global_export(*index).encode(&mut entries);
                    entries.push(0x03); // global
                    index.encode(&mut entries);
                    count += 1;
                }
                let mut contents = Vec::new();
                count.encode(&mut contents);
                contents.extend(entries);
                out.section(&wasm_encoder::RawSection {
                    id: 7,
                    data: &contents,
                });
            }
            // What start did is in the snapshot; running it again on the
            // image would apply it twice.
            Payload::StartSection { range, .. } => {
                if snapshot.is_none() {
                    copy(&mut out, 8, wasm, range)?;
                }
            }
            // Nothing in an inspected module uses the count, and the
            // image's segments are the snapshot's.
            Payload::DataCountSection { range, .. } => {
                if snapshot.is_none() {
                    copy(&mut out, 12, wasm, range)?;
                }
            }
            Payload::DataSection(section) => match snapshot {
                None => copy(&mut out, 11, wasm, section.range())?,
                Some(snapshot) => {
                    out.section(&data(&snapshot.memory));
                    data_written = true;
                }
            },
            other => return Err(format!("guest has an unsupported section: {other:?}")),
        }
    }
    if let Some(snapshot) = snapshot
        && !data_written
    {
        out.section(&data(&snapshot.memory));
    }
    Ok(out.finish())
}

/// Instructions whose effects live outside linear memory and globals.
fn changes_hidden_state(operator: &Operator<'_>) -> bool {
    matches!(
        operator,
        Operator::TableSet { .. }
            | Operator::TableGrow { .. }
            | Operator::TableFill { .. }
            | Operator::TableCopy { .. }
            | Operator::TableInit { .. }
            | Operator::ElemDrop { .. }
            | Operator::MemoryInit { .. }
            | Operator::DataDrop { .. }
    )
}

fn numeric(ty: ValType) -> bool {
    matches!(
        ty,
        ValType::I32 | ValType::I64 | ValType::F32 | ValType::F64 | ValType::V128
    )
}

fn encoder_type(ty: ValType) -> Result<wasm_encoder::ValType, String> {
    Ok(match ty {
        ValType::I32 => wasm_encoder::ValType::I32,
        ValType::I64 => wasm_encoder::ValType::I64,
        ValType::F32 => wasm_encoder::ValType::F32,
        ValType::F64 => wasm_encoder::ValType::F64,
        ValType::V128 => wasm_encoder::ValType::V128,
        ValType::Ref(_) => return Err("image global holds a reference".to_string()),
    })
}

/// The constant expression producing a snapshot value.
fn constant(value: &wasmtime::Val) -> Result<wasm_encoder::ConstExpr, String> {
    Ok(match value {
        wasmtime::Val::I32(value) => wasm_encoder::ConstExpr::i32_const(*value),
        wasmtime::Val::I64(value) => wasm_encoder::ConstExpr::i64_const(*value),
        wasmtime::Val::F32(bits) => {
            wasm_encoder::ConstExpr::f32_const(wasm_encoder::Ieee32::new(*bits))
        }
        wasmtime::Val::F64(bits) => {
            wasm_encoder::ConstExpr::f64_const(wasm_encoder::Ieee64::new(*bits))
        }
        wasmtime::Val::V128(value) => {
            wasm_encoder::ConstExpr::v128_const(i128::from_le_bytes(value.as_u128().to_le_bytes()))
        }
        _ => return Err("image global holds a reference".to_string()),
    })
}

/// Active segments holding every non-zero byte of `memory`.
fn data(memory: &[u8]) -> wasm_encoder::DataSection {
    let mut section = wasm_encoder::DataSection::new();
    for run in segments(memory) {
        // Offsets past 2 GiB wrap to negative `i32.const`s, which Wasm
        // reads back as unsigned.
        let offset = wasm_encoder::ConstExpr::i32_const(run.start as u32 as i32);
        section.active(0, &offset, memory[run].iter().copied());
    }
    section
}

/// The byte ranges of `memory` data segments must cover: every non-zero
/// byte, joining runs whose zero gaps are small, in at most
/// [`MAX_SEGMENTS`] ranges.
pub(super) fn segments(memory: &[u8]) -> Vec<Range<usize>> {
    let words = nonzero_words(memory);
    let mut gap = SEGMENT_GAP;
    loop {
        let mut runs: Vec<Range<usize>> = Vec::new();
        for word in &words {
            match runs.last_mut() {
                Some(last) if word.start - last.end <= gap => last.end = word.end,
                _ => runs.push(word.clone()),
            }
        }
        if runs.len() <= MAX_SEGMENTS {
            return runs;
        }
        gap = gap.saturating_mul(2);
    }
}

/// Maximal runs of 8-byte words of `memory` holding a non-zero byte.
fn nonzero_words(memory: &[u8]) -> Vec<Range<usize>> {
    /// Most guest memory is zero: whole blocks are skipped with one
    /// slice comparison, a `memcmp`, before any word is looked at.
    const BLOCK: usize = 4096;
    static ZERO: [u8; BLOCK] = [0; BLOCK];
    let mut runs: Vec<Range<usize>> = Vec::new();
    for (block_index, block) in memory.chunks(BLOCK).enumerate() {
        if block == &ZERO[..block.len()] {
            continue;
        }
        for (index, word) in block.chunks(8).enumerate() {
            if word == &ZERO[..word.len()] {
                continue;
            }
            let start = block_index * BLOCK + index * 8;
            let end = start + word.len();
            match runs.last_mut() {
                Some(last) if last.end == start => last.end = end,
                _ => runs.push(start..end),
            }
        }
    }
    runs
}

fn copy(
    out: &mut wasm_encoder::Module,
    id: u8,
    wasm: &[u8],
    range: Range<u64>,
) -> Result<(), String> {
    out.section(&wasm_encoder::RawSection {
        id,
        data: slice(wasm, range)?,
    });
    Ok(())
}

fn slice(wasm: &[u8], range: Range<u64>) -> Result<&[u8], String> {
    usize::try_from(range.start)
        .ok()
        .zip(usize::try_from(range.end).ok())
        .and_then(|(start, end)| wasm.get(start..end))
        .ok_or_else(|| "guest section lies outside the module".to_string())
}

fn malformed(error: wasmparser::BinaryReaderError) -> String {
    format!("guest module is malformed: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segments_cover_every_nonzero_byte() {
        let mut memory = vec![0u8; 3 * PAGE];
        memory[3] = 1;
        memory[10] = 2;
        memory[PAGE + 7] = 3;
        memory[3 * PAGE - 1] = 4;
        let runs = segments(&memory);
        // Gaps under SEGMENT_GAP join; the last byte is far away.
        assert_eq!(runs, vec![0..16, PAGE..PAGE + 8, 3 * PAGE - 8..3 * PAGE]);
        for (index, byte) in memory.iter().enumerate() {
            if *byte != 0 {
                assert!(runs.iter().any(|run| run.contains(&index)), "{index}");
            }
        }
        assert!(segments(&vec![0u8; PAGE]).is_empty());
    }

    #[test]
    fn dense_snapshots_join_wider_gaps() {
        // One non-zero byte every SEGMENT_GAP + 8 bytes: each its own run
        // at first, far more than MAX_SEGMENTS.
        let stride = SEGMENT_GAP + 16;
        let mut memory = vec![0u8; stride * (MAX_SEGMENTS * 2)];
        for at in (0..memory.len()).step_by(stride) {
            memory[at] = 1;
        }
        let runs = segments(&memory);
        assert!(runs.len() <= MAX_SEGMENTS, "{}", runs.len());
        for at in (0..memory.len()).step_by(stride) {
            assert!(runs.iter().any(|run| run.contains(&at)), "{at}");
        }
    }
}
