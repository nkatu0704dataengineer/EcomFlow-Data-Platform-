from dataclasses import dataclass, field


@dataclass(slots=True)
class Metadata:
    # Dataset Information
    dataset: str
    layer: str
    format: str

    # Storage Information
    object_path: str

    # Schema Information
    column_count: int
    columns: list[str]

    # Validation Information
    is_valid: bool
    row_count: int
    duplicate_count: int
    null_count: int
    null_details: dict[str, int]
    schema_valid: bool
    datatype_valid: bool
    

    # Processing Information
    generated_at: str
    
    error_messages: list[str] = field(default_factory=list)
    framework: str = "EcomFlow"
    framework_version: str = "3.0.0"