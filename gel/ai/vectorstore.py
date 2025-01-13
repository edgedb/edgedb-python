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

from typing import TypeVar, Any


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
        raise NotImplementedError

    def add_item(self, item: Any, metadata: dict[str, Any]) -> str:
        """
        Add a new record. The vectorstore is going to use it's embedding_model
        to generate an embedding and store it along with provided metadata.

        Returns the UUID of the inserted object.
        """
        raise NotImplementedError

    def add_vector(self, vector: list[float], metadata: dict[str, Any]) -> str:
        """
        Add a new record. The vectorstore is going to store the provided vector as is,
        as long as its dimensions match those configured in the schema.
        """
        raise NotImplementedError

    def delete(self, ids: list[str]) -> list[dict[str, Any]]:
        """
        Delete records by id. Return a list of deleted records, mirroring Gel's behaviour.
        """
        raise NotImplementedError

    def get_by_id(self, id) -> dict[str, Any]:
        """
        Get a record by its id.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def search_by_vector(
        self,
        vector: list[float],
        metadata_filter: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """
        Perform a similarity search.
        """
        raise NotImplementedError
