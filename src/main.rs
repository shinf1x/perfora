mod app;
mod config;
mod constants;
mod geometry;
mod io_utils;
mod model;
mod processing;

use app::ScanDividerApp;
use constants::APP_TITLE;
use eframe::egui;

fn main() -> eframe::Result<()> {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1620.0, 950.0]),
        ..Default::default()
    };

    eframe::run_native(
        APP_TITLE,
        native_options,
        Box::new(|cc| Ok(Box::new(ScanDividerApp::new(cc)))),
    )
}
