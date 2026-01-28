use crate::AmdSevSnpAttestation;
use crate::COCO_ATTEST_URL;
use anyhow::Context;
use anyhow::Error as AnyhowError;
use log::{debug, info};
use serde_json::Value;
use sev::firmware::guest::AttestationReport as ReprAttestationReport;
use sev_snp_utilities::guest::attestation::report::AttestationReport as LitAttestationReport;
use sha2::{Digest, Sha256};

impl AmdSevSnpAttestation {
    // Retreive the attestation report from the system.
    #[deprecated(note = "Please use CoCo v0.15+ with json_attest() instead")]
    pub async fn attest(message: &str) -> Result<LitAttestationReport, AnyhowError> {
        debug!("Starting attestation for message: {}", message);
        let serialized = Self::attest_base(message).await?;
        debug!("Received serialized data length: {}", serialized.len());
        let mut cursor = std::io::Cursor::new(serialized);
        debug!("Created cursor at position: {}", cursor.position());
        let lit_report = LitAttestationReport::from_reader(&mut cursor)
            .context("Failed to parse LitAttestationReport from cursor")?;
        debug!("Successfully parsed LitAttestationReport");
        Ok(lit_report)
    }

    pub async fn attest_base(message: &str) -> Result<Vec<u8>, AnyhowError> {
        let digest = Sha256::digest(message.as_bytes()).to_vec();
        let msg = hex::encode(digest);
        Self::attest_base_no_hash(&msg).await
    }

    pub async fn attest_base_no_hash(msg: &str) -> Result<Vec<u8>, AnyhowError> {
        let url = format!("{}/aa/evidence?runtime_data={}", COCO_ATTEST_URL, msg);
        info!("Starting old (lit_protocol/sev_snp_utilities) attestation");
        info!("Making request to URL: {}", url);

        // First check the response status
        let response = reqwest::get(url)
            .await
            .context("Failed to get AMD SEV report")?;

        let status = response.status();
        debug!("Response status: {}", status);

        // Return early if not successful
        if !status.is_success() {
            let error_text = response.text().await?;
            return Err(anyhow::anyhow!(
                "Server returned error {}: {}",
                status,
                error_text
            ));
        }

        // Now handle the successful response
        let report_text = response
            .text()
            .await
            .context("Failed to get response text")?;

        if report_text.trim().is_empty() {
            return Err(anyhow::anyhow!("Received empty response from server"));
        }

        debug!("Raw response text: {}", report_text);

        let report: Value =
            serde_json::from_str(&report_text).context("Failed to parse response as JSON")?;
        debug!("Parsed initial JSON response");

        let attestation_report = &report["attestation_report"];
        if attestation_report.is_null() {
            return Err(anyhow::anyhow!("No attestation_report field in response"));
        }
        debug!("Attestation report JSON: {}", attestation_report);

        let report = serde_json::from_value::<ReprAttestationReport>(attestation_report.clone())
            .context("Failed to parse attestation report")?;
        debug!("Successfully parsed ReprAttestationReport");

        let serialized =
            bincode::serialize(&report).context("Failed to serialize AMD SEV report")?;
        debug!("Serialized report length: {}", serialized.len());

        Ok(serialized)
    }

    // This should be used for CoCo v0.15.0+
    // Same as attest() but returns the attestation report as a JSON Value
    pub async fn json_attest(message: &str) -> Result<Value, AnyhowError> {
        debug!(
            "Starting new (trustee, json) attestation for message: {}",
            message
        );

        let digest = Sha256::digest(message.as_bytes()).to_vec();
        let msg = hex::encode(digest);

        debug!("Hashed and hex-encoded message: {}", msg);

        Self::json_attest_no_hash(&msg).await
    }

    pub async fn json_attest_no_hash(msg: &str) -> Result<Value, AnyhowError> {
        let url = format!("{}/aa/evidence?runtime_data={}", COCO_ATTEST_URL, msg);
        info!("Making request to URL: {}", url);

        // First check the response status
        let response = reqwest::get(url)
            .await
            .context("Failed to get AMD SEV report")?;
        let status = response.status();
        debug!("Response status: {}", status);

        // Return early if not successful
        if !status.is_success() {
            let error_text = response.text().await?;
            return Err(anyhow::anyhow!(
                "Server returned error {}: {}",
                status,
                error_text
            ));
        }
        // Now handle the successful response
        let report_text = response
            .text()
            .await
            .context("Failed to get response text")?;
        if report_text.trim().is_empty() {
            return Err(anyhow::anyhow!("Received empty response from server"));
        }
        debug!("Raw response text: {}", report_text);

        let report: Value =
            serde_json::from_str(&report_text).context("Failed to parse response as JSON")?;
        debug!("Parsed response as JSON");

        Ok(report)
    }
}
