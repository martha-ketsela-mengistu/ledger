# src/aggregates/document_package.py

import logging
from dataclasses import dataclass, field
from src.models.events import StoredEvent, DocumentType

logger = logging.getLogger(__name__)

@dataclass
class DocumentPackageAggregate:
    application_id: str
    documents_uploaded: set[DocumentType] = field(default_factory=set)
    extractions_completed: set[DocumentType] = field(default_factory=set)
    is_ready: bool = False
    version: int = 0

    @classmethod
    async def load(cls, store, application_id: str) -> "DocumentPackageAggregate":
        """
        Load and replay event stream to rebuild aggregate state.
        """
        logger.debug(f"Loading DocumentPackageAggregate for {application_id}")
        agg = cls(application_id=application_id)
        stream_id = f"docpkg-{application_id}"
        events = await store.load_stream(stream_id)
        for event in events:
            agg.apply(event)
        return agg

    def apply(self, event: StoredEvent) -> None:
        """
        Apply one event to update aggregate state.
        """
        logger.debug(f"[{self.application_id}] Applying {event.event_type}")
        et = event.event_type
        p = event.payload
        
        if et == "DocumentUploaded":
            dtype = p.get("document_type")
            if dtype:
                self.documents_uploaded.add(DocumentType(dtype))
        elif et == "ExtractionCompleted":
            dtype = p.get("document_type")
            if dtype:
                self.extractions_completed.add(DocumentType(dtype))
        elif et == "PackageReadyForAnalysis":
            self.is_ready = True

        self.version = event.stream_position

    def is_extraction_complete(self, required_types: list[DocumentType]) -> bool:
        """Rule 2: Verify all required documents have been extracted."""
        return all(t in self.extractions_completed for t in required_types)
