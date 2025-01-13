# Extension Vectorstore Binding
# ----------------------------
#
# The design of `GelVectorstore` follows that of the LangChain-LlamaIndex
# integration. For it to be able to interact with embedding models (outside
# of Gel) this binding also provides a simple model interface.
#
# No assumptons are made about the raw content, meaning the user might wrap
# CLIP into the interface and use it to generate and store image embeddings.
#

import gel
from typing import TypeVar, Any
from jinja2 import Template


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
with collection_records := (select {{record_type}} filter .collection = <str>$collection_name)
delete {{record_type}}
filter .id in array_unpack(<array<str>>$ids);
"""
)


SEARCH_QUERY = Template(
    """
with collection_records := (select {{record_type}} filter .collection = <str>$collection_name)
select collection_records {
    text,
    embedding,
    metadata,
    cosine_similarity := 1 - ext::pgvector::cosine_distance(
        .embedding, <ext::pgvector::vector>$query_embedding),
}
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


class BaseEmbeddingModel:
    """
    Interface for a callable that GelVectorstore is going to use
    to turn objects into embeddings.
    """

    def __call__(self, item: Any) -> list[float]:
        raise NotImplementedError

    @property
    def dimensions(self) -> int:
        raise NotImplementedError

    @property
    def target_type(self) -> TypeVar:
        """
        Returns the type that the model embeds
        """
        raise NotImplementedError


class GelVectorstore:
    """
    This class provides a set of tools to interact with Gel's ext::vectorstore
    in a framework-agnostic way.
    It follows interface conventions commonly found in vector databases.
    """

    def __init__(
        self,
        embedding_model: BaseEmbeddingModel,
        collection_name: str = "default",
        record_type: str = "ext::ai::DefaultRecord",
    ):
        self.embedding_model = embedding_model
        self.collection_name = collection_name
        self.record_type = record_type

        self._add_query = ADD_QUERY.render(self.record_type)
        self._delete_query = DELETE_BY_IDS_QUERY.render(self.record_type)
        self._search_query = SEARCH_QUERY.render(self.record_type)
        self._get_query = GET_BY_IDS_QUERY.render(self.record_type)

        self.gel_client = gel.create_client()
        raise NotImplementedError

    def verify_schema(self):
        raise NotImplementedError

    def add_item(self, item: Any, metadata: dict[str, Any]) -> str:
        """
        Add a new record. The vectorstore is going to use it's embedding_model
        to generate an embedding and store it along with provided metadata.

        Returns the UUID of the inserted object.
        """
        vector = self.embedding_model(item)
        return self.add_vector(vector=vector, raw_data=item, metadata=metadata)

    def add_vector(
        self, vector: list[float], raw_data: str, metadata: dict[str, Any]
    ) -> str:
        """
        Add a new record. The vectorstore is going to store the provided vector as is,
        as long as its dimensions match those configured in the schema.
        """

        result = self.gel_client.query(
            query=self._add_query,
            collection_name=self.collection_name,
            text=raw_data,
            embedding=vector,
            metadata=metadata,
        )
        return result

    def delete(self, ids: list[str]) -> list[dict[str, Any]]:
        """
        Delete records by id. Return a list of deleted records, mirroring Gel's behaviour.
        """
        return self.gel_client.query(
            query=self._delete_query,
            collection_name=self.collection_name,
            ids=ids,
        )

    def get_by_ids(self, ids: list[str]) -> dict[str, Any]:
        """
        Get a record by its id.
        """
        return self.gel_client.query(
            query=self._get_query,
            collection_name=self.collection_name,
            ids=ids,
        )

    def search_by_item(
        self,
        item: Any,
        metadata_filter: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """
        Create an embedding for the provided item using the embedding_model,
        then perform a similarity search.
        """
        vector = self.embedding_model(item)
        return self.search_by_vector(
            vector=vector, metadata_filter=metadata_filter, limit=limit
        )

    def search_by_vector(
        self,
        vector: list[float],
        metadata_filter: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """
        Perform a similarity search.
        """
        result = self._search_query(
            collection_name=self.collection_name,
            query_embedding=vector,
            limit=limit,
        )
        return result
