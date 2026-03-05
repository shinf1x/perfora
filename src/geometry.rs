use eframe::egui::{Color32, Pos2, Rect};

use crate::model::{PointPx, StripSettings};

const DEFAULT_LANDSCAPE_FRAME_ASPECT: f32 = 3.0 / 2.0;
const DEFAULT_PORTRAIT_FRAME_ASPECT: f32 = 2.0 / 3.0;

#[derive(Clone, Copy, Debug)]
pub struct FrameQuad {
    pub p0: PointPx,
    pub p1: PointPx,
    pub p2: PointPx,
    pub p3: PointPx,
}

impl FrameQuad {
    pub fn width_len(&self) -> f32 {
        self.p1.sub(self.p0).len()
    }

    pub fn height_len(&self) -> f32 {
        self.p2.sub(self.p0).len()
    }

    pub fn point_from_uv(&self, u: f32, v: f32) -> PointPx {
        let top = self.p0.add(self.p1.sub(self.p0).mul(u));
        let bottom = self.p2.add(self.p3.sub(self.p2).mul(u));
        top.add(bottom.sub(top).mul(v))
    }
}

pub fn strip_quad(strip: &StripSettings) -> Option<FrameQuad> {
    let [origin, along, across] = strip.corners()?;
    let p3 = along.add(across.sub(origin));
    Some(FrameQuad {
        p0: origin,
        p1: along,
        p2: across,
        p3,
    })
}

pub fn frame_quads_for_strip(strip: &StripSettings) -> Vec<FrameQuad> {
    let Some(strip_quad) = strip_quad(strip) else {
        return Vec::new();
    };

    let axes = split_and_orth_axes_for_direction(strip_quad, strip);
    let segments = frame_segments_along(strip);

    let mut quads = Vec::with_capacity(segments.len());
    for (mut start, mut end) in segments {
        if strip.direction.is_reverse() {
            let rev_start = 1.0 - end;
            let rev_end = 1.0 - start;
            start = rev_start;
            end = rev_end;
        }

        let split_start = strip_quad.p0.add(axes.split.mul(start));
        let split_end = strip_quad.p0.add(axes.split.mul(end));
        if strip.direction.is_vertical() {
            // For vertical flow, width is orthogonal to split progression.
            let p0 = split_start;
            let p1 = split_start.add(axes.orth);
            let p2 = split_end;
            let p3 = split_end.add(axes.orth);
            quads.push(FrameQuad { p0, p1, p2, p3 });
        } else {
            // For horizontal flow, width is along split progression.
            let p0 = split_start;
            let p1 = split_end;
            let p2 = split_start.add(axes.orth);
            let p3 = split_end.add(axes.orth);
            quads.push(FrameQuad { p0, p1, p2, p3 });
        }
    }

    quads
}

pub fn split_and_orth_axes(strip_quad: FrameQuad, strip: &StripSettings) -> (PointPx, PointPx) {
    let axes = split_and_orth_axes_for_direction(strip_quad, strip);
    (axes.split, axes.orth)
}

pub fn gap_boundaries_along(strip: &StripSettings) -> Vec<f32> {
    let segments = frame_segments_along(strip);
    if segments.len() <= 1 {
        return Vec::new();
    }

    let mut boundaries = Vec::with_capacity(segments.len() - 1);
    for (idx, (_, end)) in segments.into_iter().enumerate() {
        if idx + 1 >= strip.frame_count as usize {
            break;
        }
        boundaries.push(end);
    }
    boundaries
}

fn frame_segments_along(strip: &StripSettings) -> Vec<(f32, f32)> {
    if strip.frame_count == 0 {
        return Vec::new();
    }

    let frame_count = strip.frame_count as usize;
    if frame_count == 1 {
        return vec![(0.0, 1.0)];
    }

    let gap_base = strip.gap_percent.max(0.0) / 100.0;
    let denom = frame_count as f32 + (frame_count.saturating_sub(1) as f32) * gap_base;
    if denom <= 0.0 {
        return Vec::new();
    }

    let frame_u = 1.0 / denom;
    let gap_u = gap_base * frame_u;
    let min_frame_u = 0.001;

    let mut gap_starts = Vec::with_capacity(frame_count.saturating_sub(1));
    for i in 0..frame_count.saturating_sub(1) {
        let base_start = (i as f32 + 1.0) * frame_u + (i as f32) * gap_u;
        let shift_u = strip.gap_offsets.get(i).copied().unwrap_or(0.0) * frame_u;
        gap_starts.push(base_start + shift_u);
    }

    enforce_gap_lower_bounds(&mut gap_starts, gap_u, min_frame_u, false);
    enforce_gap_upper_bounds(&mut gap_starts, gap_u, min_frame_u);
    enforce_gap_lower_bounds(&mut gap_starts, gap_u, min_frame_u, true);

    let mut segments = Vec::with_capacity(frame_count);
    let mut frame_start = 0.0;
    for gap_start in gap_starts {
        let frame_end = gap_start.clamp(frame_start + min_frame_u, 1.0);
        segments.push((frame_start, frame_end));
        frame_start = (gap_start + gap_u).min(1.0);
    }
    segments.push((frame_start.min(1.0), 1.0));

    segments
}

fn enforce_gap_lower_bounds(gap_starts: &mut [f32], gap_u: f32, min_frame_u: f32, clamp: bool) {
    for i in 0..gap_starts.len() {
        let lower = gap_lower_bound(gap_starts, i, gap_u, min_frame_u);
        if gap_starts[i] < lower {
            gap_starts[i] = lower;
        }
        if clamp {
            gap_starts[i] = gap_starts[i].clamp(0.0, 1.0 - gap_u);
        }
    }
}

fn enforce_gap_upper_bounds(gap_starts: &mut [f32], gap_u: f32, min_frame_u: f32) {
    for i in (0..gap_starts.len()).rev() {
        let upper = gap_upper_bound(gap_starts, i, gap_u, min_frame_u);
        if gap_starts[i] > upper {
            gap_starts[i] = upper;
        }
    }
}

fn gap_lower_bound(gap_starts: &[f32], idx: usize, gap_u: f32, min_frame_u: f32) -> f32 {
    if idx == 0 {
        min_frame_u
    } else {
        gap_starts[idx - 1] + gap_u + min_frame_u
    }
}

fn gap_upper_bound(gap_starts: &[f32], idx: usize, gap_u: f32, min_frame_u: f32) -> f32 {
    if idx + 1 == gap_starts.len() {
        1.0 - gap_u - min_frame_u
    } else {
        gap_starts[idx + 1] - gap_u - min_frame_u
    }
}

pub fn guess_frame_count_for_strip(strip: &StripSettings, max_frames: u32) -> Option<u32> {
    let strip_quad = strip_quad(strip)?;
    let axes = split_and_orth_axes_for_direction(strip_quad, strip);
    let split_len = axes.split.len();
    let fixed_len = axes.orth.len();

    if max_frames == 0 || split_len <= 1.0 || fixed_len <= 1.0 {
        return None;
    }

    let vertical = strip.direction.is_vertical();
    // Mapping:
    // - Top/Bottom flow targets portrait frames.
    // - Left/Right flow targets landscape frames.
    let target_width_over_height = if vertical {
        DEFAULT_PORTRAIT_FRAME_ASPECT
    } else {
        DEFAULT_LANDSCAPE_FRAME_ASPECT
    };

    let mut best: Option<(u32, f32)> = None;
    for n in 1..=max_frames {
        let Some(predicted_width_over_height) =
            predicted_frame_width_over_height_for_count(strip, n, split_len, fixed_len, vertical)
        else {
            continue;
        };

        let error = (predicted_width_over_height - target_width_over_height).abs();

        match best {
            None => best = Some((n, error)),
            Some((_, best_error)) if error < best_error => best = Some((n, error)),
            _ => {}
        }
    }

    best.map(|(n, _)| n)
}

#[derive(Clone, Copy)]
struct StripAxisVectors {
    split: PointPx,
    orth: PointPx,
}

fn split_and_orth_axes_for_direction(
    strip_quad: FrameQuad,
    strip: &StripSettings,
) -> StripAxisVectors {
    let along = strip_quad.p1.sub(strip_quad.p0);
    let across = strip_quad.p2.sub(strip_quad.p0);
    let vertical = strip.direction.is_vertical();

    let along_score = directional_axis_score(along, vertical);
    let across_score = directional_axis_score(across, vertical);
    let split_is_along = if (along_score - across_score).abs() <= 0.001 {
        along.len() >= across.len()
    } else {
        along_score > across_score
    };

    if split_is_along {
        StripAxisVectors {
            split: along,
            orth: across,
        }
    } else {
        StripAxisVectors {
            split: across,
            orth: along,
        }
    }
}

fn directional_axis_score(axis: PointPx, vertical: bool) -> f32 {
    if vertical { axis.y.abs() } else { axis.x.abs() }
}

fn predicted_frame_width_over_height_for_count(
    strip: &StripSettings,
    frame_count: u32,
    split_len: f32,
    fixed_len: f32,
    vertical: bool,
) -> Option<f32> {
    if frame_count == 0 || split_len <= 0.0 || fixed_len <= 0.0 {
        return None;
    }

    let n = frame_count as usize;
    let gap_base = strip.gap_percent.max(0.0) / 100.0;
    let denom = frame_count as f32 + (n.saturating_sub(1) as f32) * gap_base;
    if denom <= 0.0 {
        return None;
    }

    let frame_split = split_len / denom;
    if vertical {
        Some(fixed_len / frame_split)
    } else {
        Some(frame_split / fixed_len)
    }
}

pub fn clamp_point_to_image(point: PointPx, source_w: f32, source_h: f32) -> PointPx {
    PointPx {
        x: point.x.clamp(0.0, (source_w - 1.0).max(0.0)),
        y: point.y.clamp(0.0, (source_h - 1.0).max(0.0)),
    }
}

pub fn source_point_to_screen(
    point: PointPx,
    image_rect: Rect,
    source_size: [usize; 2],
) -> Option<Pos2> {
    if source_size[0] == 0 || source_size[1] == 0 {
        return None;
    }

    let nx = point.x / source_size[0] as f32;
    let ny = point.y / source_size[1] as f32;

    if !nx.is_finite() || !ny.is_finite() {
        return None;
    }

    Some(Pos2::new(
        image_rect.left() + nx * image_rect.width(),
        image_rect.top() + ny * image_rect.height(),
    ))
}

pub fn screen_to_source(pos: Pos2, image_rect: Rect, source_size: [usize; 2]) -> Option<PointPx> {
    if !image_rect.contains(pos) || source_size[0] == 0 || source_size[1] == 0 {
        return None;
    }

    let nx = ((pos.x - image_rect.left()) / image_rect.width()).clamp(0.0, 1.0);
    let ny = ((pos.y - image_rect.top()) / image_rect.height()).clamp(0.0, 1.0);

    Some(PointPx {
        x: nx * source_size[0] as f32,
        y: ny * source_size[1] as f32,
    })
}

pub fn strip_color(index: usize) -> Color32 {
    match index {
        0 => Color32::from_rgb(58, 200, 110),
        1 => Color32::from_rgb(235, 150, 45),
        _ => Color32::from_rgb(200, 200, 200),
    }
}
