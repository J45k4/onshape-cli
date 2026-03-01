use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssemblyResponse {
    #[serde(rename = "rootAssembly")]
    pub root_assembly: Option<RootAssembly>,
    #[serde(rename = "subAssemblies", default)]
    pub sub_assemblies: Vec<SubAssembly>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootAssembly {
    #[serde(rename = "documentId")]
    pub document_id: Option<String>,
    #[serde(rename = "documentMicroversion")]
    pub document_microversion: Option<String>,
    #[serde(rename = "elementId")]
    pub element_id: Option<String>,
    #[serde(default)]
    pub instances: Vec<Instance>,
    #[serde(default)]
    pub occurrences: Vec<Occurrence>,
    #[serde(default)]
    pub features: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubAssembly {
    #[serde(default)]
    pub instances: Vec<Instance>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub name: Option<String>,
    pub suppressed: Option<bool>,
    #[serde(rename = "documentId")]
    pub document_id: Option<String>,
    #[serde(rename = "documentMicroversion")]
    pub document_microversion: Option<String>,
    #[serde(rename = "elementId")]
    pub element_id: Option<String>,
    #[serde(rename = "partId")]
    pub part_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Occurrence {
    #[serde(default)]
    pub path: Vec<String>,
    #[serde(default)]
    pub transform: Vec<f64>,
    pub suppressed: Option<bool>,
    pub hidden: Option<bool>,
    #[serde(rename = "isHidden")]
    pub is_hidden: Option<bool>,
    #[serde(rename = "isSuppressed")]
    pub is_suppressed: Option<bool>,
}
