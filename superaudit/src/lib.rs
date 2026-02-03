use anyhow::Result;
use chrono::{TimeZone, Utc};
use genpdf::Element;
use serde::{Deserialize, Serialize};
use std::path::Path;
use supercore::prelude::*;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuditEntry {
    pub timestamp: i64,
    pub snapshot_id: i64,
    pub operation: String,
    pub summary: std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SchemaAuditEntry {
    pub schema_id: i32,
    pub field_count: usize,
    pub fields: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuditReport {
    pub table_name: String,
    pub generated_at: i64,
    pub snapshots: Vec<AuditEntry>,
    pub schemas: Vec<SchemaAuditEntry>,
}

pub struct AuditReporter {
    table_metadata: TableMetadata,
    table_name: String,
}

impl AuditReporter {
    pub fn new(table_metadata: TableMetadata, table_name: String) -> Self {
        Self {
            table_metadata,
            table_name,
        }
    }

    pub fn generate_report(&self) -> AuditReport {
        let snapshots = self
            .table_metadata
            .snapshots
            .iter()
            .map(|s| AuditEntry {
                timestamp: self
                    .table_metadata
                    .snapshot_log
                    .iter()
                    .find(|entry| entry.snapshot_id == s.snapshot_id)
                    .map(|e| e.timestamp_ms)
                    .unwrap_or(0),
                snapshot_id: s.snapshot_id,
                operation: format!("{:?}", s.operation),
                summary: s.summary.clone(),
            })
            .collect();

        let schemas = self
            .table_metadata
            .schemas
            .iter()
            .map(|s| SchemaAuditEntry {
                schema_id: s.schema_id,
                field_count: s.fields.len(),
                fields: s
                    .fields
                    .iter()
                    .map(|f| format!("{}: {:?}", f.name, f.field_type))
                    .collect(),
            })
            .collect();

        AuditReport {
            table_name: self.table_name.clone(),
            generated_at: Utc::now().timestamp_millis(),
            snapshots,
            schemas,
        }
    }

    pub fn export_json(&self, path: impl AsRef<Path>) -> Result<()> {
        let report = self.generate_report();
        let json = serde_json::to_string_pretty(&report)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    pub fn export_markdown(&self, path: impl AsRef<Path>) -> Result<()> {
        let report = self.generate_report();
        let mut md = format!("# Audit Report: {}\n\n", report.table_name);
        md.push_str(&format!(
            "*Generated at: {}*\n\n",
            Utc.timestamp_millis_opt(report.generated_at).unwrap()
        ));

        md.push_str("## Snapshot History\n\n");
        md.push_str("| Snapshot ID | Timestamp | Operation | Summary |\n");
        md.push_str("|-------------|-----------|-----------|---------|\n");
        for s in report.snapshots {
            let ts = Utc.timestamp_millis_opt(s.timestamp).unwrap();
            md.push_str(&format!(
                "| {} | {} | {} | {:?} |\n",
                s.snapshot_id, ts, s.operation, s.summary
            ));
        }

        md.push_str("\n## Schema Evolution\n\n");
        for s in report.schemas {
            md.push_str(&format!("### Schema ID: {}\n", s.schema_id));
            md.push_str(&format!("- Fields: {}\n", s.field_count));
            for f in s.fields {
                md.push_str(&format!("  - {}\n", f));
            }
            md.push_str("\n");
        }

        std::fs::write(path, md)?;
        Ok(())
    }

    pub fn export_pdf(&self, path: impl AsRef<Path>) -> Result<()> {
        let report = self.generate_report();

        // Try common linux font paths for Ubuntu
        let font_dir = "/usr/share/fonts/truetype/ubuntu";
        let font_family = genpdf::fonts::from_files(font_dir, "Ubuntu", None)
            .map_err(|e| anyhow::anyhow!("Failed to load fonts from {}: {}", font_dir, e))?;

        let mut doc = genpdf::Document::new(font_family);
        doc.set_title(format!("SuperTable Audit: {}", report.table_name));

        let mut decorator = genpdf::SimplePageDecorator::new();
        decorator.set_margins(10);
        doc.set_page_decorator(decorator);

        doc.push(
            genpdf::elements::Text::new(format!("Audit Report: {}", report.table_name))
                .styled(genpdf::style::Color::Rgb(0, 0, 0)),
        );
        doc.push(genpdf::elements::Text::new(format!(
            "Generated: {}",
            Utc.timestamp_millis_opt(report.generated_at).unwrap()
        )));
        doc.push(genpdf::elements::Break::new(1.0));

        doc.push(
            genpdf::elements::Text::new("Snapshot History").styled(genpdf::style::Effect::Bold),
        );
        for s in report.snapshots {
            doc.push(genpdf::elements::Text::new(format!(
                "Snapshot {}: {} - {}",
                s.snapshot_id,
                s.operation,
                Utc.timestamp_millis_opt(s.timestamp).unwrap()
            )));
        }

        doc.push(genpdf::elements::Break::new(1.0));
        doc.push(
            genpdf::elements::Text::new("Schema Evolution").styled(genpdf::style::Effect::Bold),
        );
        for s in report.schemas {
            doc.push(genpdf::elements::Text::new(format!(
                "Schema {}: {} fields",
                s.schema_id, s.field_count
            )));
        }

        doc.render_to_file(path)?;
        Ok(())
    }
}
