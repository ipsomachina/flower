//! Streaming transfer of sealed objects.
//!
//! Uploads compress (optionally) and seal bytes as they arrive and send
//! them in parts: an object that fits one part goes up in one request,
//! a larger one as a multipart upload of equal parts with a shorter
//! last, so no object is held whole and none is bounded by one
//! request's limit. Downloads open sealed frames as their bytes arrive
//! and release only authenticated plaintext.

use std::io::Write;

use bytes::Bytes;
use flower_crypto::aead::{CryptoError, STREAM_HEADER_LEN, StreamOpener};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path as ObjPath;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload};

use crate::error::BackupError;
use crate::object::{BackupKey, FRAME_LEN, ZSTD_LEVEL};

/// What one upload stored.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Uploaded {
    /// Key version sealing the object.
    pub version: u32,
    /// Bytes sealed: the compressed bytes when compressing.
    pub sealed_input: u64,
    /// Bytes stored.
    pub stored: u64,
}

/// One sealed object going up in parts. [`Upload::write`] and
/// [`Upload::finish`] abort a begun multipart upload when they fail;
/// a caller failing between them calls [`Upload::abort`]. One dropped
/// unfinished, its future cancelled (a shutdown that stops waiting, a
/// caller's timeout), aborts in the background: S3 would otherwise keep
/// the parts of an upload nothing completes, which no listing shows.
pub(crate) struct Upload<'a> {
    store: &'a dyn ObjectStore,
    path: ObjPath,
    part_bytes: usize,
    version: u32,
    sink: Option<Sink>,
    multipart: Option<Box<dyn MultipartUpload>>,
    stored: u64,
}

impl<'a> Upload<'a> {
    /// Start uploading `name`, sealed under the current key version in
    /// frames no longer than a part, compressed first when `compress`.
    pub(crate) fn begin(
        store: &'a dyn ObjectStore,
        key: &BackupKey,
        name: &str,
        part_bytes: usize,
        compress: bool,
    ) -> Result<Self, BackupError> {
        let part_bytes = part_bytes.max(1);
        let (version, sealer) = key
            .sealer(part_bytes.min(FRAME_LEN))
            .map_err(BackupError::Crypto)?;
        let sealing = Sealing { sealer, input: 0 };
        let sink = if compress {
            Sink::Zstd(zstd::stream::write::Encoder::new(sealing, ZSTD_LEVEL).map_err(io_error)?)
        } else {
            Sink::Plain(sealing)
        };
        Ok(Upload {
            store,
            path: ObjPath::from(name),
            part_bytes,
            version,
            sink: Some(sink),
            multipart: None,
            stored: 0,
        })
    }

    /// Add bytes, uploading every part they fill.
    pub(crate) async fn write(&mut self, bytes: &[u8]) -> Result<(), BackupError> {
        let written = self.write_parts(bytes).await;
        if written.is_err() {
            self.abort_begun().await;
        }
        written
    }

    /// Seal the rest and store the object: one request when it fits a
    /// part, else its last parts and the multipart completion.
    pub(crate) async fn finish(mut self) -> Result<Uploaded, BackupError> {
        let finished = self.complete().await;
        if finished.is_err() {
            self.abort_begun().await;
        }
        finished
    }

    /// Give the upload up, removing the parts a begun multipart upload
    /// stored.
    pub(crate) async fn abort(mut self) {
        self.abort_begun().await;
    }

    /// The sink, until finishing takes it.
    fn sink(&mut self) -> Result<&mut Sink, BackupError> {
        self.sink
            .as_mut()
            .ok_or_else(|| BackupError::Io("upload already finished".to_string()))
    }

    async fn write_parts(&mut self, mut bytes: &[u8]) -> Result<(), BackupError> {
        while !bytes.is_empty() {
            let take = bytes.len().min(self.part_bytes);
            self.sink()?.write_all(&bytes[..take]).map_err(io_error)?;
            bytes = &bytes[take..];
            // The last part stays behind: it may be the object's only
            // one, which needs no multipart upload.
            while self.sink()?.sealing().sealer.ready() > self.part_bytes {
                let part_bytes = self.part_bytes;
                let part = self.sink()?.sealing().sealer.take(part_bytes);
                self.put_part(part).await?;
            }
        }
        Ok(())
    }

    async fn complete(&mut self) -> Result<Uploaded, BackupError> {
        let sink = self
            .sink
            .take()
            .ok_or_else(|| BackupError::Io("upload already finished".to_string()))?;
        let sealing = sink.finish().map_err(io_error)?;
        let sealed_input = sealing.input;
        let mut rest = sealing.sealer.finish().map_err(BackupError::Crypto)?;
        while rest.len() > self.part_bytes {
            let tail = rest.split_off(self.part_bytes);
            self.put_part(std::mem::replace(&mut rest, tail)).await?;
        }
        self.stored += rest.len() as u64;
        match self.multipart.as_mut() {
            None => {
                self.store
                    .put(&self.path, PutPayload::from(rest))
                    .await
                    .map_err(store_error)?;
            }
            Some(multipart) => {
                multipart
                    .put_part(PutPayload::from(rest))
                    .await
                    .map_err(store_error)?;
                multipart.complete().await.map_err(store_error)?;
                self.multipart = None;
            }
        }
        Ok(Uploaded {
            version: self.version,
            sealed_input,
            stored: self.stored,
        })
    }

    /// Upload one full part, beginning the multipart upload at the first.
    async fn put_part(&mut self, part: Vec<u8>) -> Result<(), BackupError> {
        if self.multipart.is_none() {
            let begun = self
                .store
                .put_multipart(&self.path)
                .await
                .map_err(store_error)?;
            self.multipart = Some(begun);
        }
        let Some(multipart) = self.multipart.as_mut() else {
            unreachable!("begun above");
        };
        self.stored += part.len() as u64;
        multipart
            .put_part(PutPayload::from(part))
            .await
            .map_err(store_error)
    }

    /// Abort a begun multipart upload. A store that cannot abort keeps
    /// the parts, which a bucket lifecycle rule for incomplete multipart
    /// uploads removes; nothing ever names them.
    async fn abort_begun(&mut self) {
        if let Some(multipart) = self.multipart.take() {
            abort_upload(&self.path, multipart).await;
        }
    }
}

impl Drop for Upload<'_> {
    fn drop(&mut self) {
        let Some(multipart) = self.multipart.take() else {
            return;
        };
        let path = self.path.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move { abort_upload(&path, multipart).await });
            }
            Err(_) => tracing::warn!(
                target: "flower::backup",
                object = %path,
                "an unfinished multipart upload dropped outside a runtime stays unaborted"
            ),
        }
    }
}

/// Abort a multipart upload, warning when the store cannot.
async fn abort_upload(path: &ObjPath, mut multipart: Box<dyn MultipartUpload>) {
    if let Err(error) = multipart.abort().await {
        tracing::warn!(
            target: "flower::backup",
            object = %path,
            error = %error,
            "could not abort an unfinished multipart upload"
        );
    }
}

/// Seals what it is given, counting it.
struct Sealing {
    sealer: flower_crypto::aead::StreamSealer,
    input: u64,
}

impl Write for Sealing {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.sealer.write(bytes).map_err(std::io::Error::other)?;
        self.input += bytes.len() as u64;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Where an upload's bytes go: straight to sealing, or through zstd.
enum Sink {
    Plain(Sealing),
    Zstd(zstd::stream::write::Encoder<'static, Sealing>),
}

impl Sink {
    fn sealing(&mut self) -> &mut Sealing {
        match self {
            Sink::Plain(sealing) => sealing,
            Sink::Zstd(encoder) => encoder.get_mut(),
        }
    }

    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match self {
            Sink::Plain(sealing) => sealing.write_all(bytes),
            Sink::Zstd(encoder) => encoder.write_all(bytes),
        }
    }

    fn finish(self) -> std::io::Result<Sealing> {
        match self {
            Sink::Plain(sealing) => Ok(sealing),
            Sink::Zstd(encoder) => encoder.finish(),
        }
    }
}

/// One sealed object coming down, opened frame by frame.
pub(crate) struct Download {
    stream: BoxStream<'static, object_store::Result<Bytes>>,
    key: BackupKey,
    header: Vec<u8>,
    opener: Option<StreamOpener>,
    ended: bool,
}

impl Download {
    /// Start downloading `name`; a missing object is
    /// [`BackupError::NotFound`].
    pub(crate) async fn begin(
        store: &dyn ObjectStore,
        key: &BackupKey,
        name: &str,
    ) -> Result<Self, BackupError> {
        let result = store.get(&ObjPath::from(name)).await.map_err(store_error)?;
        Ok(Download {
            stream: result.into_stream(),
            key: key.clone(),
            header: Vec::with_capacity(STREAM_HEADER_LEN),
            opener: None,
            ended: false,
        })
    }

    /// The next authenticated plaintext, `None` once the last frame
    /// opened.
    pub(crate) async fn next(&mut self) -> Result<Option<Vec<u8>>, BackupError> {
        if self.ended {
            return Ok(None);
        }
        let mut opened = Vec::new();
        while opened.is_empty() {
            let Some(chunk) = self.stream.next().await else {
                self.ended = true;
                let opener = self
                    .opener
                    .take()
                    .ok_or(BackupError::Crypto(CryptoError::Truncated))?;
                opener.finish(&mut opened).map_err(BackupError::Crypto)?;
                return Ok(Some(opened));
            };
            let chunk = chunk.map_err(store_error)?;
            let mut rest = &chunk[..];
            if self.opener.is_none() {
                let take = (STREAM_HEADER_LEN - self.header.len()).min(rest.len());
                self.header.extend_from_slice(&rest[..take]);
                rest = &rest[take..];
                if self.header.len() < STREAM_HEADER_LEN {
                    continue;
                }
                self.opener = Some(self.key.opener(&self.header).map_err(BackupError::Crypto)?);
            }
            if let Some(opener) = self.opener.as_mut() {
                opener
                    .feed(rest, &mut opened)
                    .map_err(BackupError::Crypto)?;
            }
        }
        Ok(Some(opened))
    }

    /// Every opened byte, decompressed when `compressed`.
    pub(crate) async fn read_all(mut self, compressed: bool) -> Result<Vec<u8>, BackupError> {
        if !compressed {
            let mut all = Vec::new();
            while let Some(bytes) = self.next().await? {
                all.extend(bytes);
            }
            return Ok(all);
        }
        let mut decoder = zstd::stream::write::Decoder::new(Vec::new()).map_err(io_error)?;
        while let Some(bytes) = self.next().await? {
            decoder.write_all(&bytes).map_err(io_error)?;
        }
        decoder.flush().map_err(io_error)?;
        Ok(decoder.into_inner())
    }
}

/// Map an object-store failure, keeping missing objects distinct.
pub(crate) fn store_error(error: object_store::Error) -> BackupError {
    match error {
        object_store::Error::NotFound { path, .. } => BackupError::NotFound(path),
        error => BackupError::Store(error.to_string()),
    }
}

/// Map a compression or spool IO failure.
pub(crate) fn io_error(error: std::io::Error) -> BackupError {
    BackupError::Io(error.to_string())
}
