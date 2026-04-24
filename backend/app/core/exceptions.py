"""Custom exceptions."""


class DocFusionError(Exception):
    """Base exception."""


class DocumentReadError(DocFusionError):
    """Failed to read a document."""


class TemplateParseError(DocFusionError):
    """Failed to parse a template."""


class LLMError(DocFusionError):
    """LLM call failed."""


class ExtractionError(DocFusionError):
    """Data extraction failed."""


class FillError(DocFusionError):
    """Template filling failed."""


class ValidationError(DocFusionError):
    """Validation failed."""


class SourceConnectError(DocFusionError):
    """External source connection failed."""


class NormalizationError(DocFusionError):
    """Normalization failed."""


class QualityCheckError(DocFusionError):
    """Quality analysis failed."""


class DocumentOperationError(DocFusionError):
    """Document operation failed."""
