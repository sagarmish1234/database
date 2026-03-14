use std::fs::File;

use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct DbConfig {
    pub file_path: String,
    pub page_size: usize,
    pub cache_size: usize,
    pub port: usize,
}

impl DbConfig {
    pub fn new(file_path: &str) -> Result<Self> {
        let file = File::open(file_path)?;
        Ok(serde_yaml::from_reader(file)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_config_load_success() -> Result<()> {
        // Create a temporary file
        let mut file = NamedTempFile::new()?;
        let yaml_content = "
        file_path: '/data/db.bin'
        page_size: 4096
        cache_size: 1024
        port: 8080
        ";
        writeln!(file, "{}", yaml_content)?;

        // Test the loader
        let path = file.path().to_str().unwrap();
        let config = DbConfig::new(path)?;

        assert_eq!(config.file_path, "/data/db.bin");
        assert_eq!(config.page_size, 4096);
        assert_eq!(config.port, 8080);

        Ok(())
    }

    #[test]
    fn test_config_invalid_yaml() -> Result<()> {
        let mut file = NamedTempFile::new()?;
        // Missing 'port' field
        let yaml_content = "
        file_path: '/data/db.bin'
        page_size: 4096
        cache_size: 1024
        ";
        writeln!(file, "{}", yaml_content)?;

        let path = file.path().to_str().unwrap();
        let result = DbConfig::new(path);

        // Should fail because port is required
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_config_file_not_found() {
        let result = DbConfig::new("non_existent_file.yaml");
        assert!(result.is_err());
    }
}
