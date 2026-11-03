//! Latency histograms with logarithmic buckets.
//!
//! Bucket upper bounds are 0, then 0.001 ms growing by 1% up to 24 hours,
//! then infinity: about 2,530 buckets, so a reported percentile is at most 1%
//! above the true value between a microsecond and a day. Percentiles are
//! nearest-rank bucket upper bounds, capped at the largest sample. Histograms
//! combine by adding bucket counts, never by averaging percentiles, so
//! intervals, phases and runs merge exactly. All histograms use the same
//! bounds, so runs line up bucket for bucket.

use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

/// Smallest positive bound, in milliseconds.
pub const MIN_POSITIVE_MS: f64 = 0.001;
/// Largest finite bound: 24 hours in milliseconds.
pub const MAX_BOUNDED_MS: f64 = 86_400_000.0;
/// Growth from one bound to the next.
pub const BUCKET_RATIO: f64 = 1.01;

/// Every bucket's upper bound, in milliseconds; the last is infinity.
pub fn bounds() -> &'static [f64] {
    static BOUNDS: OnceLock<Vec<f64>> = OnceLock::new();
    BOUNDS.get_or_init(|| {
        let mut values = vec![0.0, MIN_POSITIVE_MS];
        while values[values.len() - 1] < MAX_BOUNDED_MS {
            let next = (values[values.len() - 1] * BUCKET_RATIO).min(MAX_BOUNDED_MS);
            values.push(next);
        }
        values.push(f64::INFINITY);
        values
    })
}

/// The bucket a sample falls in: the first whose bound is at least it.
fn bucket(value: f64) -> usize {
    bounds().partition_point(|bound| *bound < value)
}

/// A latency histogram in milliseconds.
#[derive(Clone, Debug, PartialEq)]
pub struct Histogram {
    buckets: Vec<u64>,
    samples: u64,
    invalid: u64,
    min: f64,
    max: f64,
}

impl Default for Histogram {
    fn default() -> Self {
        Histogram {
            buckets: vec![0; bounds().len()],
            samples: 0,
            invalid: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }
}

/// The percentiles every report states.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Percentiles {
    /// Median.
    pub p50: Option<f64>,
    /// 90th percentile.
    pub p90: Option<f64>,
    /// 99th percentile.
    pub p99: Option<f64>,
    /// 99.9th percentile.
    pub p999: Option<f64>,
    /// Largest sample.
    pub max: Option<f64>,
}

/// A histogram on the wire: nonzero buckets as `[index, count]` pairs
/// into [`bounds`], plus exact extremes. Stable across releases: the
/// bounds are fixed by [`BUCKET_RATIO`] and its limits.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistogramSnapshot {
    /// Valid samples recorded.
    pub samples: u64,
    /// Negative or non-finite samples, left out of every percentile.
    pub invalid_samples: u64,
    /// Smallest sample.
    pub min: Option<f64>,
    /// The reported percentiles and the largest sample.
    #[serde(flatten)]
    pub percentiles: Percentiles,
    /// Nonzero buckets as `[index, count]`.
    pub buckets: Vec<(usize, u64)>,
}

impl Histogram {
    /// Record one sample. Negative or non-finite samples count as
    /// invalid and stay out of every percentile.
    pub fn record(&mut self, value: f64) {
        if !value.is_finite() || value < 0.0 {
            self.invalid += 1;
            return;
        }
        self.buckets[bucket(value)] += 1;
        self.samples += 1;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    /// Add another histogram's samples to this one.
    pub fn merge(&mut self, other: &Histogram) {
        for (mine, theirs) in self.buckets.iter_mut().zip(&other.buckets) {
            *mine += theirs;
        }
        self.samples += other.samples;
        self.invalid += other.invalid;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    /// Valid samples recorded.
    pub fn samples(&self) -> u64 {
        self.samples
    }

    /// The nearest-rank percentile `percent` (0 to 100) as its bucket's
    /// upper bound, capped at the largest sample; `None` when empty.
    pub fn percentile(&self, percent: f64) -> Option<f64> {
        if self.samples == 0 {
            return None;
        }
        let rank = ((percent / 100.0) * self.samples as f64).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (index, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return Some(bounds()[index].min(self.max));
            }
        }
        Some(self.max)
    }

    /// p50, p90, p99, p99.9 and the maximum.
    pub fn percentiles(&self) -> Percentiles {
        Percentiles {
            p50: self.percentile(50.0),
            p90: self.percentile(90.0),
            p99: self.percentile(99.0),
            p999: self.percentile(99.9),
            max: (self.samples > 0).then_some(self.max),
        }
    }

    /// The wire form.
    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            samples: self.samples,
            invalid_samples: self.invalid,
            min: (self.samples > 0).then_some(self.min),
            percentiles: self.percentiles(),
            buckets: self
                .buckets
                .iter()
                .enumerate()
                .filter(|(_, count)| **count > 0)
                .map(|(index, count)| (index, *count))
                .collect(),
        }
    }

    /// Rebuild a histogram from its wire form; `None` when a bucket
    /// index is out of range or the counts disagree.
    pub fn from_snapshot(snapshot: &HistogramSnapshot) -> Option<Histogram> {
        let mut histogram = Histogram::default();
        for (index, count) in &snapshot.buckets {
            *histogram.buckets.get_mut(*index)? += count;
            histogram.samples += count;
        }
        if histogram.samples != snapshot.samples {
            return None;
        }
        histogram.invalid = snapshot.invalid_samples;
        histogram.min = snapshot.min.unwrap_or(f64::INFINITY);
        histogram.max = snapshot.percentiles.max.unwrap_or(f64::NEG_INFINITY);
        Some(histogram)
    }

    /// Counts of the nonzero buckets with their upper bounds, for charts.
    pub fn nonzero(&self) -> impl Iterator<Item = (f64, u64)> + '_ {
        self.buckets
            .iter()
            .enumerate()
            .filter(|(_, count)| **count > 0)
            .map(|(index, count)| (bounds()[index].min(self.max), *count))
    }
}

/// How the histograms are bucketed, for reports.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistogramScheme {
    /// Unit of every value.
    pub unit: &'static str,
    /// How percentiles are read.
    pub percentiles: &'static str,
    /// Relative width of one bucket.
    pub relative_bucket_width: f64,
    /// Smallest positive bound.
    pub min_positive_ms: f64,
    /// Largest finite bound.
    pub max_bounded_ms: f64,
    /// Buckets, the infinite one included.
    pub bucket_count: usize,
}

/// The scheme every histogram here uses.
pub fn scheme() -> HistogramScheme {
    HistogramScheme {
        unit: "milliseconds",
        percentiles: "nearest-rank logarithmic bucket upper bounds, capped at the maximum",
        relative_bucket_width: BUCKET_RATIO - 1.0,
        min_positive_ms: MIN_POSITIVE_MS,
        max_bounded_ms: MAX_BOUNDED_MS,
        bucket_count: bounds().len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_match_v1() {
        let bounds = bounds();
        assert_eq!(bounds[0], 0.0);
        assert_eq!(bounds[1], 0.001);
        assert_eq!(bounds[bounds.len() - 2], MAX_BOUNDED_MS);
        assert!(bounds[bounds.len() - 1].is_infinite());
        // Every finite step grows by at most 1%.
        for pair in bounds[1..bounds.len() - 1].windows(2) {
            assert!(pair[1] / pair[0] <= BUCKET_RATIO + 1e-12, "{pair:?}");
        }
        assert_eq!(bounds.len(), 2534);
    }

    #[test]
    fn zero_submicrosecond_and_overflow_samples_land_in_their_buckets() {
        // Known samples pin the bucket boundaries and percentile values.
        let mut histogram = Histogram::default();
        for value in [0.0, 0.0, 0.0001, 0.0002, 0.001, 0.001, 1.0, 1.0, 1e9, 2e9] {
            histogram.record(value);
        }
        assert_eq!(histogram.percentile(20.0), Some(0.0));
        assert_eq!(histogram.percentile(30.0), Some(0.001));
        assert_eq!(histogram.percentile(90.0), Some(2e9));
        assert_eq!(histogram.buckets[histogram.buckets.len() - 1], 2);
    }

    #[test]
    fn percentiles_are_within_one_bucket_of_the_truth() {
        let mut histogram = Histogram::default();
        for value in 1..=10_000 {
            histogram.record(f64::from(value) / 10.0);
        }
        let p = histogram.percentiles();
        let near = |got: Option<f64>, want: f64| {
            let got = got.unwrap();
            assert!(got >= want && got <= want * BUCKET_RATIO, "{got} vs {want}");
        };
        near(p.p50, 500.0);
        near(p.p90, 900.0);
        near(p.p99, 990.0);
        near(p.p999, 999.0);
        assert_eq!(p.max, Some(1000.0));
        // Never above the maximum.
        assert!(histogram.percentile(100.0).unwrap() <= 1000.0);
    }

    #[test]
    fn invalid_samples_are_counted_apart() {
        let mut histogram = Histogram::default();
        histogram.record(f64::NAN);
        histogram.record(-1.0);
        histogram.record(f64::INFINITY);
        assert_eq!(histogram.samples(), 0);
        assert_eq!(histogram.percentile(50.0), None);
        assert_eq!(histogram.snapshot().invalid_samples, 3);
    }

    #[test]
    fn merging_buckets_differs_from_averaging_percentiles() {
        // A fast interval and a slow one: the merged p99 is the slow
        // tail, which no average of the two p99s gives.
        let mut fast = Histogram::default();
        for _ in 0..990 {
            fast.record(1.0);
        }
        let mut slow = Histogram::default();
        for _ in 0..10 {
            slow.record(500.0);
        }
        let mut merged = fast.clone();
        merged.merge(&slow);
        assert_eq!(merged.samples(), 1000);
        let p99 = merged.percentile(99.0).unwrap();
        assert!((1.0..=1.01).contains(&p99), "{p99}");
        let p999 = merged.percentile(99.9).unwrap();
        assert_eq!(p999, 500.0);
        let averaged = (fast.percentile(99.9).unwrap() + slow.percentile(99.9).unwrap()) / 2.0;
        assert!((averaged - p999).abs() > 100.0);
        // Merging in any order gives the same histogram.
        let mut other = slow.clone();
        other.merge(&fast);
        assert_eq!(other, merged);
    }

    #[test]
    fn snapshots_round_trip() {
        let mut histogram = Histogram::default();
        for value in [0.5, 2.0, 2.0, 40.0, 1e6] {
            histogram.record(value);
        }
        let snapshot = histogram.snapshot();
        let json = serde_json::to_string(&snapshot).unwrap();
        let back: HistogramSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(Histogram::from_snapshot(&back).unwrap(), histogram);
        let mut broken = back.clone();
        broken.buckets.push((usize::MAX, 1));
        assert!(Histogram::from_snapshot(&broken).is_none());
    }
}
