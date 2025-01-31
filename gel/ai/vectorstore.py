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
from typing import Optional, TypeVar, Any
from jinja2 import Template
from metadata_filters import (
    MetadataFilter,
    MetadataFilters,
    FilterOperator,
    FilterCondition,
)

ADD_QUERY = Template(
    """
    select (
        insert {{record_type}} {
            collection := <str>$collection_name,
            text := <str>$text,
            embedding := <array<float32>>$embedding,
            metadata := <json>$metadata,
        }
    )
    """.strip()
)

DELETE_BY_IDS_QUERY = Template(
    """
    delete {{record_type}}
    filter .id in array_unpack(<array<str>>$ids) 
    and .collection = <str>$collection_name;
    """
)

SEARCH_QUERY = Template(
    """
    with collection_records := (
        select {{record_type}} 
        filter .collection = <str>$collection_name
    )
    select collection_records {
        text,
        embedding,
        metadata,
        cosine_similarity := 1 - ext::pgvector::cosine_distance(
            .embedding, <ext::pgvector::vector>$query_embedding),
    }
    {{metadata_filter}}
    order by .cosine_similarity desc empty last
    limit <optional int64>$limit;
    """
)

GET_BY_IDS_QUERY = Template(
    """
    select {{record_type}} {
        text,
        embedding,
        metadata,
    }
    filter .id in array_unpack(<array<str>>$ids);
    """
)


def get_filter_clause(filters: MetadataFilters) -> str:
    subclauses = []
    for filter in filters.filters:
        subclause = ""

        if isinstance(filter, MetadataFilters):
            subclause = get_filter_clause(filter)
        elif isinstance(filter, MetadataFilter):
            formatted_value = (
                f'"{filter.value}"'
                if isinstance(filter.value, str)
                else filter.value
            )

            match filter.operator:
                case (
                    FilterOperator.EQ
                    | FilterOperator.NE
                    | FilterOperator.GT
                    | FilterOperator.GTE
                    | FilterOperator.LT
                    | FilterOperator.LTE
                    | FilterOperator.LIKE
                    | FilterOperator.ILIKE
                ):
                    subclause = (
                        f'<str>json_get(.metadata, "{filter.key}") '
                        f"{filter.operator.value} {formatted_value}"
                    )

                case FilterOperator.IN | FilterOperator.NOT_IN:
                    subclause = (
                        f'<str>json_get(.metadata, "{filter.key}") '
                        f"{filter.operator.value} "
                        f"array_unpack({formatted_value})"
                    )

                case FilterOperator.ANY | FilterOperator.ALL:
                    subclause = (
                        f"{filter.operator.value}"
                        f'(<str>json_get(.metadata, "{filter.key}") = '
                        f"array_unpack({formatted_value}))"
                    )

                case FilterOperator.CONTAINS | FilterOperator.EXISTS:
                    subclause = (
                        f'contains(<str>json_get(.metadata, "{filter.key}"), '
                        f"{formatted_value})"
                    )
                case _:
                    raise ValueError(f"Unknown operator: {filter.operator}")

        subclauses.append(subclause)

    if filters.condition in {FilterCondition.AND, FilterCondition.OR}:
        filter_clause = f" {filters.condition.value} ".join(subclauses)
        return (
            "(" + filter_clause + ")" if len(subclauses) > 1 else filter_clause
        )
    else:
        raise ValueError(f"Unknown condition: {filters.condition}")


class BaseEmbeddingModel:
    """
    Abstract base class for embedding models.

    Any embedding model used with `GelVectorstore` must implement this
    interface. The model is expected to convert input data (text, images, etc.)
    into a numerical vector representation.
    """

    def __call__(self, item: Any) -> list[float]:
        """
        Convert an input item into a list of floating-point values (vector
        embedding). Must be implemented in subclasses.
        """
        raise NotImplementedError

    @property
    def dimensions(self) -> int:
        """
        Return the number of dimensions in the embedding vector.
        Must be implemented in subclasses.
        """
        raise NotImplementedError

    @property
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
        embedding_model: BaseEmbeddingModel,
        collection_name: str = "default",
        record_type: str = "ext::ai::DefaultRecord",
    ):
        """
        Initialize the vector store.

        Args:
            embedding_model (BaseEmbeddingModel): The embedding model used to 
                generate vectors.
            collection_name (str): The name of the collection.
            record_type (str): The schema type (table name) for storing records.
        """
        self.embedding_model = embedding_model
        self.collection_name = collection_name
        self.record_type = record_type

        self.gel_client = gel.create_client()
        raise NotImplementedError

    def verify_schema(self):
        """Verify that the database schema is correctly configured."""
        raise NotImplementedError

    def add_item(self, item: Any, metadata: dict[str, Any]) -> str:
        """
        Add a new record.

        The vectorstore is going to use it's embedding_model to generate an
        embedding , and store it along with provided metadata.

        Args:
            item (Any): The input data to be embedded.
            metadata (dict): Additional metadata for the record.

        Returns:
            str: The UUID of the inserted object.
        """
        vector = self.embedding_model(item)
        return self.add_vector(vector=vector, raw_data=item, metadata=metadata)

    def add_vector(
        self, vector: list[float], raw_data: str, metadata: dict[str, Any]
    ) -> str:
        """
        Add a precomputed vector to the vector store.

        Args:
            vector (list[float]): The numerical vector representation of the 
                item.
            raw_data (str): The original input data.
            metadata (dict): Additional metadata.

        Returns:
            str: The UUID of the inserted object.
        """
        result = self.gel_client.query(
            query=ADD_QUERY.render(self.record_type),
            collection_name=self.collection_name,
            text=raw_data,
            embedding=vector,
            metadata=metadata,
        )
        return result

    def delete(self, ids: list[str]) -> list[dict[str, Any]]:
        """
        Delete records by their IDs.

        Args:
            ids (list[str]): A list of record IDs to delete.

        Returns:
            list[dict]: A list of deleted records.
        """
        return self.gel_client.query(
            query=DELETE_BY_IDS_QUERY.render(self.record_type),
            collection_name=self.collection_name,
            ids=ids,
        )

    def get_by_ids(self, ids: list[str]) -> dict[str, Any]:
        """
        Retrieve records by their IDs.

        Args:
            ids (list[str]): A list of record IDs to retrieve.

        Returns:
            dict: The retrieved records.
        """
        return self.gel_client.query(
            query= GET_BY_IDS_QUERY.render(self.record_type),
            collection_name=self.collection_name,
            ids=ids,
        )

    def search_by_item(
        self,
        item: Any,
        filters: Optional[MetadataFilters] = None,
        limit: int = 4,
    ) -> list[dict[str, Any]]:
        """
        Perform a similarity search based on an input item.

        The embedding model generates an embedding for the provided item,
        and the vector store searches for the most similar records.

        Args:
            item (Any): The input item to be embedded.
            metadata_filter (str): A filter for metadata-based search.
            limit (int): Maximum number of results to return. Defaults to 4.

        Returns:
            list[dict]: A list of the most similar records.
        """
        vector = self.embedding_model(item)
        metadata_filter = f"filter {get_filter_clause(filters)}" if filters else ""
        return self.search_by_vector(
            vector=vector, metadata_filter=metadata_filter, limit=limit
        )

    def search_by_vector(
        self,
        vector: list[float],
        metadata_filter: str,
        limit: int = 4,
    ) -> list[dict[str, Any]]:
        """
        Perform a similarity search using a precomputed vector.

        Args:
            vector (list[float]): The query embedding vector.
            metadata_filter (str): A filter for metadata-based search.
            limit (int): Maximum number of results to return. Defaults to 4.

        Returns:
            list[dict]: A list of the most similar records.
        """

        result = self.gel_client.query(
            query=SEARCH_QUERY.render(self.record_type, metadata_filter),
            collection_name=self.collection_name,
            query_embedding=vector,
            limit=limit,
        )

        return result
