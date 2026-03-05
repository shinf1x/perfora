#[derive(Clone, Copy, PartialEq, Eq)]
pub enum OutputBitDepth {
    PreserveSource,
    Convert8Bit,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ExportImageFormat {
    PreserveSource,
    Jpeg,
    Png,
    Tiff,
}

impl ExportImageFormat {
    pub const ALL: [ExportImageFormat; 4] = [
        ExportImageFormat::PreserveSource,
        ExportImageFormat::Jpeg,
        ExportImageFormat::Png,
        ExportImageFormat::Tiff,
    ];

    pub fn label(self) -> &'static str {
        match self {
            ExportImageFormat::PreserveSource => "Preserve source format",
            ExportImageFormat::Jpeg => "JPEG",
            ExportImageFormat::Png => "PNG",
            ExportImageFormat::Tiff => "TIFF",
        }
    }
}

impl OutputBitDepth {
    pub const ALL: [OutputBitDepth; 2] =
        [OutputBitDepth::PreserveSource, OutputBitDepth::Convert8Bit];

    pub fn label(self) -> &'static str {
        match self {
            OutputBitDepth::PreserveSource => "Preserve source bit depth",
            OutputBitDepth::Convert8Bit => "Convert to 8-bit",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FrameDirection {
    LeftToRight,
    RightToLeft,
    TopToBottom,
    BottomToTop,
}

impl FrameDirection {
    pub const ALL: [FrameDirection; 4] = [
        FrameDirection::LeftToRight,
        FrameDirection::RightToLeft,
        FrameDirection::TopToBottom,
        FrameDirection::BottomToTop,
    ];

    pub fn label(self) -> &'static str {
        match self {
            FrameDirection::LeftToRight => "Left -> Right",
            FrameDirection::RightToLeft => "Right -> Left",
            FrameDirection::TopToBottom => "Top -> Bottom",
            FrameDirection::BottomToTop => "Bottom -> Top",
        }
    }

    pub fn is_reverse(self) -> bool {
        matches!(
            self,
            FrameDirection::RightToLeft | FrameDirection::BottomToTop
        )
    }

    pub fn is_vertical(self) -> bool {
        matches!(
            self,
            FrameDirection::TopToBottom | FrameDirection::BottomToTop
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PointPx {
    pub x: f32,
    pub y: f32,
}

impl PointPx {
    pub fn add(self, other: PointPx) -> PointPx {
        PointPx {
            x: self.x + other.x,
            y: self.y + other.y,
        }
    }

    pub fn sub(self, other: PointPx) -> PointPx {
        PointPx {
            x: self.x - other.x,
            y: self.y - other.y,
        }
    }

    pub fn mul(self, scalar: f32) -> PointPx {
        PointPx {
            x: self.x * scalar,
            y: self.y * scalar,
        }
    }

    pub fn len(self) -> f32 {
        (self.x * self.x + self.y * self.y).sqrt()
    }
}

#[derive(Clone, Debug)]
pub struct StripSettings {
    pub corner_origin: Option<PointPx>,
    pub corner_along: Option<PointPx>,
    pub corner_across: Option<PointPx>,
    pub frame_count: u32,
    pub gap_percent: f32,
    pub gap_offsets: Vec<f32>,
    pub direction: FrameDirection,
}

impl Default for StripSettings {
    fn default() -> Self {
        Self {
            corner_origin: None,
            corner_along: None,
            corner_across: None,
            frame_count: 6,
            gap_percent: 2.5,
            gap_offsets: vec![0.0; 5],
            direction: FrameDirection::TopToBottom,
        }
    }
}

impl StripSettings {
    pub fn corners(&self) -> Option<[PointPx; 3]> {
        Some([self.corner_origin?, self.corner_along?, self.corner_across?])
    }

    pub fn set_corners_from_rect(&mut self, x: f32, y: f32, w: f32, h: f32) {
        self.corner_origin = Some(PointPx { x, y });
        self.corner_along = Some(PointPx { x: x + w, y });
        self.corner_across = Some(PointPx { x, y: y + h });
    }

    pub fn clear_corners(&mut self) {
        self.corner_origin = None;
        self.corner_along = None;
        self.corner_across = None;
    }

    pub fn ensure_gap_offsets_len(&mut self) {
        let needed = self.frame_count.saturating_sub(1) as usize;
        if self.gap_offsets.len() < needed {
            self.gap_offsets.resize(needed, 0.0);
        } else if self.gap_offsets.len() > needed {
            self.gap_offsets.truncate(needed);
        }
    }
}

#[derive(Clone, Debug)]
pub struct ImageSettings {
    pub strip_count: usize,
    pub strips: [StripSettings; 2],
}

impl ImageSettings {
    pub fn new_default(source_w: f32, source_h: f32) -> Self {
        let mut strip_1 = StripSettings::default();
        strip_1.set_corners_from_rect(0.0, 0.0, source_w, source_h);

        Self {
            strip_count: 2,
            strips: [strip_1, StripSettings::default()],
        }
    }
}

#[derive(Clone, Default)]
pub struct ExportMetadata {
    pub camera_make: String,
    pub camera_model: String,
    pub author: String,
    pub scan_datetime: String,
    pub film_stock: String,
    pub image_description: String,
    pub notes: String,
}

#[derive(Clone)]
pub struct ExportSettings {
    pub auto_contrast_enabled: bool,
    pub low_percentile: f32,
    pub high_percentile: f32,
    pub auto_contrast_sample_area_percent: f32,
    pub mirror: bool,
    pub invert_colors: bool,
    pub bit_depth: OutputBitDepth,
    pub format: ExportImageFormat,
    pub metadata: ExportMetadata,
}

impl Default for ExportSettings {
    fn default() -> Self {
        Self {
            auto_contrast_enabled: true,
            low_percentile: 1.0,
            high_percentile: 99.0,
            auto_contrast_sample_area_percent: 90.0,
            mirror: false,
            invert_colors: false,
            bit_depth: OutputBitDepth::Convert8Bit,
            format: ExportImageFormat::Png,
            metadata: ExportMetadata::default(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum InteractionMode {
    #[default]
    None,
    PickStripCorner {
        strip_index: usize,
        corner_index: usize,
    },
}

impl InteractionMode {
    pub fn label(self) -> &'static str {
        match self {
            InteractionMode::None => "Idle",
            InteractionMode::PickStripCorner {
                strip_index,
                corner_index,
            } => {
                let strip = strip_index + 1;
                let corner = corner_index + 1;
                if strip == 1 {
                    if corner == 1 {
                        "Pick Strip 1 Corner 1"
                    } else if corner == 2 {
                        "Pick Strip 1 Corner 2"
                    } else {
                        "Pick Strip 1 Corner 3"
                    }
                } else if corner == 1 {
                    "Pick Strip 2 Corner 1"
                } else if corner == 2 {
                    "Pick Strip 2 Corner 2"
                } else {
                    "Pick Strip 2 Corner 3"
                }
            }
        }
    }
}
