# Extension Vectorstore Binding
# ----------------------------
#
# `GelVectorstore` is designed to integrate with vector databases following
# LangChain-LlamaIndex conventions. It enables interaction with embedding models
# (both within and outside of Gel) through a simple interface.
#
# This binding does not assume a specific data type, allowing it to support
# text, images, or any other embeddings. For example, CLIP can be wrapped into
# this interface to generate and store image embeddings.

import gel
import json
import uuid
from dataclasses import dataclass, field
from abc import abstractmethod
from typing import Optional, TypeVar, Any, List, Dict, Generic
from jinja2 import Template
from .metadata_filter import (
    get_filter_clause,
    CompositeFilter,
)


BATCH_ADD_QUERY = Template(
    """
    with items := json_array_unpack(<json>$items)
    select (
        for item in items union (
            insert {{record_type}} {
                collection := <str>$collection_name,
                text := <optional str>item['text'],
                embedding := <optional array<float32>>item['embedding'],
                metadata := <optional json>item['metadata']
            }
        )
    )
    """.strip()
)

DELETE_BY_IDS_QUERY = Template(
    """
    delete {{record_type}}
    filter .id in array_unpack(<array<uuid>>$ids) 
    and .collection = <str>$collection_name;
    """.strip()
)

SEARCH_QUERY = Template(
    """
    with collection_records := (
        select {{record_type}} 
        filter .collection = <str>$collection_name
        and exists(.embedding)
    )
    select collection_records {
        id,
        text,
        embedding,
        metadata,
        cosine_similarity := 1 - ext::pgvector::cosine_distance(
            .embedding, <ext::pgvector::vector>$query_embedding),
    }
    {{filter_expression}}
    order by .cosine_similarity desc empty last
    limit <optional int64>$limit;
    """
)

GET_BY_IDS_QUERY = Template(
    """
    select {{record_type}} {
        id, 
        text,
        embedding,
        metadata,
    }
    filter .id in array_unpack(<array<uuid>>$ids)
    and .collection = <str>$collection_name;
    """.strip()
)

UPDATE_QUERY = Template(
    """
    with updates := array_unpack(<array<str>>$updates)
    update {{record_type}}
    filter .id = <uuid>$id and .collection = <str>$collection_name
    set {
        text := <optional str>$text if 'text' in updates else .text,
        embedding := <optional ext::pgvector::vector>$embedding 
            if 'embedding' in updates 
            else .embedding,
        metadata := <optional json>$metadata 
            if 'metadata' in updates 
            else .metadata,
    };
    """.strip()
)


@dataclass
class ItemToInsert:
    """An item whose embedding will be created and stored
    alongside the item in the vector store."""

    text: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecordToInsert:
    """A record to be added to the vector store with embedding pre-computed."""

    embedding: List[float]
    text: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IdRecord:
    """A database record identifier returned from insert,
    update, or delete operations."""

    id: uuid.UUID


@dataclass(init=False)
class Record(IdRecord):
    """A record retrieved from the vector store, or an update record.

    Custom `__init__` so we can detect which fields the user passed
    (even if they pass None or {}).
    """

    text: Optional[str] = None
    embedding: Optional[List[float]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # We'll fill these dynamically in __init__
    _explicitly_set_fields: set = field(default_factory=set, repr=False)

    def __init__(self, id: uuid.UUID, **kwargs):
        """
        Force the user to provide `id` positionally/explicitly,
        then capture any *other* fields in **kwargs.
        """
        # First, initialize IdRecord:
        super().__init__(id=id)

        # For text, embedding, metadata, we use what's in kwargs
        # or fall back to the default already on the class.
        self.text = kwargs.get("text", None)
        self.embedding = kwargs.get("embedding", None)
        self.metadata = kwargs.get("metadata", {})

        # Mark which fields were actually passed by the user (ignore 'id').
        # So if user calls Record(id=..., text=None), "text" will appear here.
        object.__setattr__(self, "_explicitly_set_fields", set(kwargs.keys()))

    def is_field_set(self, field: str) -> bool:
        """Check if a field was explicitly set in the constructor call."""
        return field in self._explicitly_set_fields


@dataclass
class SearchResult(Record):
    """A search result from the vector store."""

    cosine_similarity: float = 0.0


T = TypeVar("T")


class BaseEmbeddingModel(Generic[T]):
    """
    Abstract base class for embedding models.

    Any embedding model used with `GelVectorstore` must implement this
    interface. The model is expected to convert input data (text, images, etc.)
    into a numerical vector representation.
    """

    @abstractmethod
    def __call__(self, item: T) -> List[float]:
        """
        Convert an input item into a list of floating-point values (vector
        embedding). Must be implemented in subclasses.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def dimensions(self) -> int:
        """
        Return the number of dimensions in the embedding vector.
        Must be implemented in subclasses.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def target_type(self) -> TypeVar:
        """
        Return the expected data type of the input (e.g., str for text, image
        for vision models). Must be implemented in subclasses.
        """
        raise NotImplementedError


class GelVectorstore:
    """
    A framework-agnostic interface for interacting with Gel's ext::vectorstore.

    This class provides methods for storing, retrieving, and searching
    vector embeddings. It follows vector database conventions and supports
    different embedding models.
    """

    def __init__(
        self,
        embedding_model: Optional[BaseEmbeddingModel] = None,
        collection_name: str = "default",
        record_type: str = "ext::vectorstore::DefaultRecord",
        client_config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize a new vector store instance.

        Args:
            embedding_model (BaseEmbeddingModel): The embedding model used to
              generate vectors.
            collection_name (str): The name of the collection.
            record_type (str): The schema type (table name) for storing records.
            client_config (Optional[dict]): The config for the Gel client.
        """
        self.embedding_model = embedding_model
        self.collection_name = collection_name
        self.record_type = record_type
        self.gel_client = gel.create_client(**(client_config or {}))

    def add_items(self, items: List[ItemToInsert]) -> List[IdRecord]:
        """
        Add multiple items to the vector store in a single transaction.
        Embeddinsg will be generated and stored for all items.

        Args:
            items (List[ItemToInsert]): List of items to add. Each contains:
                - text (str): The text content to be embedded
                - metadata (Dict[str, Any]): Additional data to store

        Returns:
            List[IdRecord]: List of database records for the inserted items.
        """
        items_with_embeddings = [
            RecordToInsert(
                text=item.text,
                embedding=(
                    self.embedding_model(item.text) if item.text else None
                ),
                metadata=item.metadata,
            )
            for item in items
        ]
        return self.add_vectors(items_with_embeddings)

    def add_vectors(self, records: List[RecordToInsert]) -> List[IdRecord]:
        """Add pre-computed vector embeddings to the store.

        Use this method when you have already generated embeddings and want to
        store them directly without re-computing them.

        Args:
            records (List[RecordToInsert]): List of records. Each contains:
                - embedding ([List[float]): Pre-computed embeddings
                - text (Optional[str]): Original text content
                - metadata ([Dict[str, Any]): Additional data to store

        Returns:
            List[IdRecord]: List of database records for the inserted items.
        """
        results = self.gel_client.query(
            query=BATCH_ADD_QUERY.render(record_type=self.record_type),
            collection_name=self.collection_name,
            items=json.dumps(
                [
                    {
                        "text": record.text,
                        "embedding": record.embedding,
                        "metadata": record.metadata or {},
                    }
                    for record in records
                ]
            ),
        )
        return [IdRecord(id=result.id) for result in results]

    def delete(self, ids: List[uuid.UUID]) -> List[IdRecord]:
        """Delete records from the vector store by their IDs.

        Args:
            ids (List[uuid.UUID]): List of record IDs to delete.

        Returns:
            List[IdRecord]: List of deleted records, containing their IDs.
        """
        results = self.gel_client.query(
            query=DELETE_BY_IDS_QUERY.render(record_type=self.record_type),
            collection_name=self.collection_name,
            ids=ids,
        )
        return [IdRecord(id=result.id) for result in results]

    def get_by_ids(self, ids: List[uuid.UUID]) -> List[Record]:
        """Retrieve specific records by their IDs.

        Args:
            ids (List[uuid.UUID]): List of record IDs to retrieve.

        Returns:
            List[Record]: List of retrieved records. Each result contains:
                - id (uuid.UUID): The record's unique identifier
                - text (Optional[str]): The original text content
                - embedding (Optional[List[float]]): The stored vector embedding
                - metadata (Optional[Dict[str, Any]]): Any associated metadata
        """
        results = self.gel_client.query(
            query=GET_BY_IDS_QUERY.render(record_type=self.record_type),
            collection_name=self.collection_name,
            ids=ids,
        )
        return [
            Record(
                id=result.id,
                text=result.text,
                embedding=result.embedding and list(result.embedding),
                metadata=(json.loads(result.metadata)),
            )
            for result in results
        ]

    def search_by_item(
        self,
        item: Any,
        filters: Optional[CompositeFilter] = None,
        limit: Optional[int] = 4,
    ) -> List[SearchResult]:
        """Search for similar items in the vector store.

        This method:
        1. Generates an embedding for the input item
        2. Finds records with similar embeddings
        3. Optionally filters results based on metadata
        4. Returns the most similar items up to the specified limit

        Args:
            item (Any): The query item to find similar matches for.
              Must be compatible with the embedding model's target_type.
            filters (Optional[CompositeFilter]): Metadata-based filters to use.
            limit (Optional[int]): Max number of results to return.
              Defaults to 4.

        Returns:
            List[SearchResult]: List of similar items, ordered by similarity.
                Each result contains:
                - id (uuid.UUID): The record's unique identifier
                - text (Optional[str]): The original text content
                - embedding (List[float]): The stored vector embedding
                - metadata (Optional[Dict[str, Any]]): Any associated metadata
                - cosine_similarity (float): Similarity score
                  (higher is more similar)
        """
        vector = self.embedding_model(item)
        filter_expression = (
            f"filter {get_filter_clause(filters)}" if filters else ""
        )
        return self.search_by_vector(
            vector=vector, filter_expression=filter_expression, limit=limit
        )

    def search_by_vector(
        self,
        vector: List[float],
        filter_expression: str = "",
        limit: Optional[int] = 4,
    ) -> List[SearchResult]:
        """Search using a pre-computed vector embedding.

        Useful when you have already computed the embedding or want to search
        with a modified/combined embedding vector.

        Args:
            vector (List[float]): The query embedding to search with.
              Must match the dimensionality of stored embeddings.
            filter_expression (str): Filter expression for metadata filtering.
            limit (Optional[int]): Max number of results to return.
              Defaults to 4.

        Returns:
            List[SearchResult]: List of similar items, ordered by similarity.
                Each result contains:
                - id (uuid.UUID): The record's unique identifier
                - text (Optional[str]): The original text content
                - embedding (List[float]): The stored vector embedding
                - metadata (Optional[Dict[str, Any]]): Any associated metadata
                - cosine_similarity (float): Similarity score
                  (higher is more similar)
        """
        results = self.gel_client.query(
            query=SEARCH_QUERY.render(
                record_type=self.record_type,
                filter_expression=filter_expression,
            ),
            collection_name=self.collection_name,
            query_embedding=vector,
            limit=limit,
        )
        return [
            SearchResult(
                id=result.id,
                text=result.text,
                embedding=list(result.embedding) if result.embedding else None,
                metadata=json.loads(result.metadata),
                cosine_similarity=result.cosine_similarity,
            )
            for result in results
        ]

    def update_record(self, record: Record) -> Optional[IdRecord]:
        """Update an existing record in the vector store.

        Only specified fields will be updated. If text is provided but not
        embedding, a new embedding will be automatically generated.

        Args:
            Record:
                - id (uuid.UUID): The ID of the record to update
                - text (Optional[str]): New text content. If provided without
                  embedding, a new embedding will be generated.
                - embedding (Optional[List[float]]): New vector embedding.
                - metadata (Optional[Dict[str, Any]]): New metadata to store
                  with the record. Completely replaces existing metadata.
        Returns:
            Optional[IdRecord]: The updated record's ID if found and updated,
              None if no record was found with the given ID.
        Raises:
            ValueError: If no fields are specified for update.
        """
        if not any(
            record.is_field_set(field)
            for field in ["text", "embedding", "metadata"]
        ):
            raise ValueError("No fields specified for update.")

        updates = {
            field
            for field in ["text", "embedding", "metadata"]
            if record.is_field_set(field)
        }

        if "text" in updates and "embedding" not in updates:
            updates.add("embedding")
            record.embedding = self.embedding_model(record.text)

        result = self.gel_client.query_single(
            query=UPDATE_QUERY.render(record_type=self.record_type),
            collection_name=self.collection_name,
            id=record.id,
            updates=list(updates),
            text=record.text,
            embedding=record.embedding,
            metadata=json.dumps(record.metadata or {}),
        )
        return IdRecord(id=result.id) if result else None
