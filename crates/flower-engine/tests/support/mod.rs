//! Hand-assembled guests for the runtime's image and pool tests: they
//! pin exactly what a guest writes where, which an SDK guest cannot.
//!
//! Every guest imports `flower.host_call`, exports `memory`,
//! `flower_alloc`, `flower_invoke`, `flower_manifest` and `flower_init`,
//! and keeps envelopes in data segments: [`OK_AT`] holds the ok
//! envelope and [`failure_at`] one failure envelope per entry of
//! [`FAILURES`].

#![allow(dead_code)]

use flower_engine::runtime::HostCall;
use flower_sdk::{Value, fail};

/// Where the ok envelope lives.
pub const OK_AT: u32 = 1024;
/// Where `flower_alloc` places every allocation.
pub const ALLOC_AT: u32 = 16 * 1024;
/// Failure codes a guest can answer with, each at [`failure_at`].
pub const FAILURES: [&str; 9] = [
    "LEAK_COUNTER",
    "LEAK_HIDDEN",
    "LEAK_CONSTANT",
    "LEAK_FLOAT",
    "LEAK_INIT",
    "LEAK_START",
    "LEAK_GROWN",
    "LEAK_SIZE",
    "LEAK_DIRTY",
];

/// Value types.
pub const I32: u8 = 0x7f;
pub const I64: u8 = 0x7e;
pub const F64: u8 = 0x7c;

/// Function types: (i32 i32 i32) -> i64, (i32) -> i32, () -> i64, () -> ().
pub const INVOKE_TYPE: u8 = 0;
pub const ALLOC_TYPE: u8 = 1;
pub const PACKED_TYPE: u8 = 2;
pub const START_TYPE: u8 = 3;

pub fn uleb(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

pub fn sleb(mut value: i64, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        let done = (value == 0 && byte & 0x40 == 0) || (value == -1 && byte & 0x40 != 0);
        out.push(if done { byte } else { byte | 0x80 });
        if done {
            return;
        }
    }
}

fn name(text: &str, out: &mut Vec<u8>) {
    uleb(text.len() as u64, out);
    out.extend_from_slice(text.as_bytes());
}

fn section(id: u8, body: Vec<u8>, out: &mut Vec<u8>) {
    out.push(id);
    uleb(body.len() as u64, out);
    out.extend(body);
}

pub fn ok_envelope() -> Vec<u8> {
    flower_sdk::abi::encode_ok(Value::Null)
        .encode_binary()
        .unwrap()
}

fn failure_envelope(code: &str) -> Vec<u8> {
    flower_sdk::abi::encode_err(&fail(code, "guest state leaked"))
        .encode_binary()
        .unwrap()
}

/// Where the failure envelope for `code` lives.
pub fn failure_at(code: &str) -> u32 {
    let index = FAILURES.iter().position(|known| *known == code).unwrap();
    2048 + 128 * index as u32
}

/// The packed `ptr << 32 | len` of the envelope at `at`.
pub fn packed(at: u32, envelope: &[u8]) -> u64 {
    (u64::from(at) << 32) | envelope.len() as u64
}

/// Function body bytes, built instruction by instruction.
#[derive(Clone, Default)]
pub struct Code(pub Vec<u8>);

impl Code {
    pub fn new() -> Self {
        Code::default()
    }

    pub fn raw(mut self, bytes: &[u8]) -> Self {
        self.0.extend_from_slice(bytes);
        self
    }

    pub fn i32(mut self, value: i32) -> Self {
        self.0.push(0x41);
        sleb(i64::from(value), &mut self.0);
        self
    }

    pub fn i64(mut self, value: i64) -> Self {
        self.0.push(0x42);
        sleb(value, &mut self.0);
        self
    }

    pub fn f64(mut self, value: f64) -> Self {
        self.0.push(0x44);
        self.0.extend(value.to_le_bytes());
        self
    }

    pub fn local_get(mut self, index: u32) -> Self {
        self.0.push(0x20);
        uleb(u64::from(index), &mut self.0);
        self
    }

    pub fn global_get(mut self, index: u32) -> Self {
        self.0.push(0x23);
        uleb(u64::from(index), &mut self.0);
        self
    }

    pub fn global_set(mut self, index: u32) -> Self {
        self.0.push(0x24);
        uleb(u64::from(index), &mut self.0);
        self
    }

    /// Push the byte at `at`.
    pub fn load8(self, at: u32) -> Self {
        self.i32(at as i32).raw(&[0x2d, 0x00, 0x00])
    }

    /// Store `value`'s low byte at `at`.
    pub fn store8(self, at: u32, value: i32) -> Self {
        self.i32(at as i32).i32(value).raw(&[0x3a, 0x00, 0x00])
    }

    /// Grow memory by `pages`, dropping the result.
    pub fn grow(self, pages: i32) -> Self {
        self.i32(pages).raw(&[0x40, 0x00, 0x1a])
    }

    /// Return the packed envelope at `at`.
    pub fn answer(self, at: u32, envelope: &[u8]) -> Self {
        self.i64(packed(at, envelope) as i64).raw(&[0x0f])
    }

    /// Return the ok envelope.
    pub fn ok(self) -> Self {
        self.answer(OK_AT, &ok_envelope())
    }

    /// Pop an i32 condition; when nonzero, return the failure `code`.
    pub fn fail_if(self, code: &str) -> Self {
        self.raw(&[0x04, 0x40])
            .answer(failure_at(code), &failure_envelope(code))
            .raw(&[0x0b])
    }

    /// Pop an i32 condition; when nonzero, run `then`.
    pub fn when(self, then: Code) -> Self {
        self.raw(&[0x04, 0x40]).raw(&then.0).raw(&[0x0b])
    }
}

/// A defined function: its type, i32 locals and body.
pub struct Func {
    pub ty: u8,
    pub locals: u32,
    pub body: Code,
}

/// A hand-assembled guest. Function 0 is the `host_call` import; the
/// required exports point at `alloc`, `invoke`, `manifest` and `init`.
pub struct Guest {
    /// Initial memory pages.
    pub pages: u32,
    pub invoke: Func,
    pub init: Option<Func>,
    pub start: Option<Code>,
    /// (type, mutable, init expression without `end`).
    pub globals: Vec<(u8, bool, Code)>,
    /// Extra exports: (name, kind, index).
    pub exports: Vec<(String, u8, u32)>,
    /// Whether the guest defines a one-element funcref table.
    pub table: bool,
}

impl Guest {
    pub fn new(invoke: Code) -> Self {
        Guest {
            pages: 1,
            invoke: Func {
                ty: INVOKE_TYPE,
                locals: 0,
                body: invoke,
            },
            init: None,
            start: None,
            globals: Vec::new(),
            exports: Vec::new(),
            table: false,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = b"\0asm\x01\0\0\0".to_vec();
        let mut types = vec![4];
        types.extend([0x60, 3, I32, I32, I32, 1, I64]);
        types.extend([0x60, 1, I32, 1, I32]);
        types.extend([0x60, 0, 1, I64]);
        types.extend([0x60, 0, 0]);
        section(1, types, &mut out);
        let mut imports = vec![1];
        name("flower", &mut imports);
        name("host_call", &mut imports);
        imports.extend([0x00, INVOKE_TYPE]);
        section(2, imports, &mut out);
        // Functions 1 alloc, 2 invoke, 3 manifest, then init and start.
        let mut functions: Vec<(u8, u32, Code)> = vec![
            (ALLOC_TYPE, 0, Code::new().i32(ALLOC_AT as i32)),
            (self.invoke.ty, self.invoke.locals, self.invoke.body.clone()),
            (PACKED_TYPE, 0, Code::new().ok()),
        ];
        let mut exports: Vec<(String, u8, u32)> = vec![
            ("memory".to_string(), 2, 0),
            ("flower_alloc".to_string(), 0, 1),
            ("flower_invoke".to_string(), 0, 2),
            ("flower_manifest".to_string(), 0, 3),
        ];
        if let Some(init) = &self.init {
            exports.push(("flower_init".to_string(), 0, functions.len() as u32 + 1));
            functions.push((init.ty, init.locals, init.body.clone()));
        }
        let start = self.start.as_ref().map(|body| {
            functions.push((START_TYPE, 0, body.clone()));
            functions.len() as u32
        });
        exports.extend(self.exports.iter().cloned());
        let mut function_section = Vec::new();
        uleb(functions.len() as u64, &mut function_section);
        function_section.extend(functions.iter().map(|(ty, _, _)| *ty));
        section(3, function_section, &mut out);
        if self.table {
            section(4, vec![1, 0x70, 0x00, 1], &mut out);
        }
        let mut memory = vec![1, 0x00];
        uleb(u64::from(self.pages), &mut memory);
        section(5, memory, &mut out);
        if !self.globals.is_empty() {
            let mut globals = Vec::new();
            uleb(self.globals.len() as u64, &mut globals);
            for (ty, mutable, init) in &self.globals {
                globals.extend([*ty, u8::from(*mutable)]);
                globals.extend(&init.0);
                globals.push(0x0b);
            }
            section(6, globals, &mut out);
        }
        let mut export_section = Vec::new();
        uleb(exports.len() as u64, &mut export_section);
        for (export, kind, index) in &exports {
            name(export, &mut export_section);
            export_section.push(*kind);
            uleb(u64::from(*index), &mut export_section);
        }
        section(7, export_section, &mut out);
        if let Some(start) = start {
            let mut body = Vec::new();
            uleb(u64::from(start), &mut body);
            section(8, body, &mut out);
        }
        let mut code = Vec::new();
        uleb(functions.len() as u64, &mut code);
        for (_, locals, instructions) in &functions {
            let mut body = Vec::new();
            if *locals == 0 {
                body.push(0);
            } else {
                body.push(1);
                uleb(u64::from(*locals), &mut body);
                body.push(I32);
            }
            body.extend(&instructions.0);
            body.push(0x0b);
            uleb(body.len() as u64, &mut code);
            code.extend(body);
        }
        section(10, code, &mut out);
        let mut segments = vec![(OK_AT, ok_envelope())];
        for code in FAILURES {
            segments.push((failure_at(code), failure_envelope(code)));
        }
        let mut data = Vec::new();
        uleb(segments.len() as u64, &mut data);
        for (at, bytes) in segments {
            data.extend([0x00, 0x41]);
            sleb(i64::from(at), &mut data);
            data.push(0x0b);
            uleb(bytes.len() as u64, &mut data);
            data.extend(bytes);
        }
        section(11, data, &mut out);
        out
    }
}

/// A host answering every call with an error envelope.
pub struct NoHost;

impl HostCall for NoHost {
    fn op(&mut self, _op: u8, _input: &[u8]) -> Vec<u8> {
        flower_sdk::abi::encode_err(&fail("NO_HOST", "no host in this test"))
            .encode_binary()
            .unwrap()
    }
}
