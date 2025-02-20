#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2025-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Extension Vectorstore Binding
# ----------------------------
#
# `Vectorstore` is designed to integrate with vector databases following
# LangChain-LlamaIndex conventions. It enables interaction with embedding models
# (both within and outside of Gel) through a simple interface.
#
# This binding does not assume a specific data type, allowing it to support
# text, images, or any other embeddings. For example, CLIP can be wrapped into
# this interface to generate and store image embeddings.


from __future__ import annotations
from typing import (
    Optional,
    TypeVar,
    Any,
    List,
    Dict,
    Generic,
    Self,
    Union,
    overload,
    TYPE_CHECKING,
    Coroutine,
)

import abc
import array
import dataclasses
import enum
import json
import textwrap
import uuid

from gel import abstract
from gel import errors
from gel import quote
from gel.protocol import protocol


if TYPE_CHECKING:
    try:
        import numpy as np
        import numpy.typing as npt

        Vector = Union[
            List[float], array.array[float], npt.NDArray[np.float32]
        ]
    except ImportError:
        Vector = Union[List[float], array.array[float]]


class Query(abstract.AsQueryWithArgs):
    def __init__(self, query: str, **kwargs):
        self.query = query
        self.kwargs = kwargs

    def as_query_with_args(self, *args, **kwargs) -> abstract.QueryWithArgs:
        if args:
            raise errors.InvalidArgumentError(
                "this query does not accept positional arguments"
            )
        return abstract.QueryWithArgs(
            query=self.query,
            args=args,
            kwargs={**self.kwargs, **kwargs},
            input_language=protocol.InputLanguage.EdgeQL,
        )


@dataclasses.dataclass(kw_only=True)
class AddRecord(abstract.AsQueryWithArgs):
    """A record to be added to the vector store with embedding pre-computed."""

    record_type: str
    collection_name: str
    embedding: Vector
    text: Optional[str]
    metadata: Optional[Dict[str, Any]]

    def asdict(self, json_compat: bool = False, **override) -> Dict[str, Any]:
        rv = dataclasses.asdict(self)
        rv.pop("record_type")
        if self.metadata is not None:
            rv["metadata"] = json.dumps(self.metadata)
        rv.update(override)
        if json_compat and hasattr(rv["embedding"], "tolist"):
            rv["embedding"] = rv["embedding"].tolist()
        return rv

    def as_query_with_args(self, *args, **kwargs) -> abstract.QueryWithArgs:
        if args:
            raise errors.InvalidArgumentError(
                "this query does not accept positional arguments"
            )
        return abstract.QueryWithArgs(
            query=textwrap.dedent(
                f"""
                insert {quote.quote_ident(self.record_type)} {{
                    collection := <str>$collection_name,
                    text := <optional str>$text,
                    embedding := <optional ext::pgvector::vector>$embedding,
                    metadata := <optional json>$metadata,
                }}
                """
            ),
            args=args,
            kwargs=self.asdict(**kwargs),
            input_language=protocol.InputLanguage.EdgeQL,
        )


class AddRecords(abstract.AsQueryWithArgs):
    """Add multiple records to the vector store in a single transaction."""

    def __init__(self, *records: AddRecord):
        record_type = set(record.record_type for record in records)
        if len(record_type) == 0:
            raise errors.InvalidArgumentError("no records provided")
        if len(record_type) > 1:
            raise errors.InvalidArgumentError(
                f"all records must have the same record type, "
                f"got {record_type}"
            )
        self.record_type = record_type.pop()
        self.records = records

    def as_query_with_args(self, *args, **kwargs) -> abstract.QueryWithArgs:
        if args:
            raise errors.InvalidArgumentError(
                "this query does not accept positional arguments"
            )
        return abstract.QueryWithArgs(
            query=textwrap.dedent(
                f"""
                with items := json_array_unpack(<json>$items)
                for item in items union (
                    insert {quote.quote_ident(self.record_type)} {{
                        collection := <str>item['collection_name'],
                        text := <str>item['text'],
                        embedding := <array<float32>>item['embedding'],
                        metadata := to_json(<str>item['metadata']),
                    }}
                )
                """
            ),
            args=args,
            kwargs={
                "items": json.dumps(
                    [
                        r.asdict(json_compat=True, **kwargs)
                        for r in self.records
                    ]
                )
            },
            input_language=protocol.InputLanguage.EdgeQL,
        )


JsonValue = Union[int, float, str, bool]


class FilterOperator(str, enum.Enum):
    EQ = "="
    NE = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="


class SearchRecordQuery(abstract.AsQueryWithArgs):
    def __init__(
        self,
        record_type: str,
        collection_name: str,
        embedding: Vector,
        limit: int = 4,
    ):
        self.record_type = record_type
        self.collection_name = collection_name
        self.embedding = embedding
        self.limit = limit
        self.filter_args = []
        self.filters = []

    def filter(self, expr: str, *args: Any) -> Self:
        if self.filter_args and args:
            raise errors.InvalidArgumentError(
                "filter() with arguments can only be called once before "
                "adding any filter with arguments"
            )
        self.filters.append(f"({expr})")
        self.filter_args.extend(args)
        return self

    @overload
    def filter_metadata(
        self, *path: str, eq: JsonValue, default: bool = False
    ) -> Self: ...

    @overload
    def filter_metadata(
        self, *path: str, ne: JsonValue, default: bool = False
    ) -> Self: ...

    @overload
    def filter_metadata(
        self, *path: str, gt: JsonValue, default: bool = False
    ) -> Self: ...

    @overload
    def filter_metadata(
        self, *path: str, gte: JsonValue, default: bool = False
    ) -> Self: ...

    @overload
    def filter_metadata(
        self, *path: str, lt: JsonValue, default: bool = False
    ) -> Self: ...

    @overload
    def filter_metadata(
        self, *path: str, lte: JsonValue, default: bool = False
    ) -> Self: ...

    def filter_metadata(self, *path: str, **op_vals: JsonValue) -> Self:
        if not path:
            raise errors.InterfaceError(
                "at least one path element is required"
            )

        default = str(op_vals.pop("default", False)).lower()
        if len(op_vals) != 1:
            raise errors.InterfaceError(
                "expected exactly one operator-value pair"
            )

        op_str, value = op_vals.popitem()
        if isinstance(value, str):
            typ = "str"
        elif isinstance(value, bool):  # bool is a subclass of int, goes first
            typ = "bool"
        elif isinstance(value, int):
            typ = "int64"
        elif isinstance(value, float):
            typ = "float64"
        else:
            raise errors.InterfaceError(
                f"unsupported value type: {type(value).__name__}"
            )

        path_param = ", ".join(quote.quote_literal(p) for p in path)
        left = f"<{typ}>json_get(.metadata, {path_param})"
        op = FilterOperator(op_str.upper())
        right = f"<{typ}>${len(self.filter_args)}"
        self.filters.append(f"(({left} {op} {right}) ?? {default})")
        self.filter_args.append(value)

        return self

    def limit(self, limit: int) -> Self:
        self.limit = limit
        return self

    def as_query_with_args(self, *args, **kwargs) -> abstract.QueryWithArgs:
        if args:
            raise errors.InvalidArgumentError(
                "this query does not accept positional arguments"
            )
        c = len(self.filter_args)
        if self.filters:
            filter_expression = "filter " + " and ".join(self.filters)
        else:
            filter_expression = ""
        return abstract.QueryWithArgs(
            query=textwrap.dedent(
                f"""
                with collection_records := (
                    select {quote.quote_ident(self.record_type)}
                        filter .collection = <str>${c}
                            and exists(.embedding)
                )
                select collection_records {{
                    id,
                    text,
                    embedding,
                    metadata,
                    cosine_similarity := 1 - ext::pgvector::cosine_distance(
                        .embedding, <ext::pgvector::vector>${c + 1}),
                }}
                {filter_expression}
                order by .cosine_similarity desc empty last
                limit <optional int64>${c + 2};
                """
            ),
            args=(
                *self.filter_args,
                kwargs.pop("collection_name", self.collection_name),
                kwargs.pop("embedding", self.embedding),
                kwargs.pop("limit", self.limit),
            ),
            kwargs={},
            input_language=protocol.InputLanguage.EdgeQL,
        )


T = TypeVar("T")


class BaseEmbeddingModel(abc.ABC, Generic[T]):
    @property
    @abc.abstractmethod
    def dimensions(self) -> int:
        """
        Return the number of dimensions in the embedding vector.
        Must be implemented in subclasses.
        """
        ...

    @property
    @abc.abstractmethod
    def target_type(self) -> TypeVar:
        """
        Return the expected data type of the input (e.g., str for text, image
        for vision models). Must be implemented in subclasses.
        """
        ...

    """
    Abstract base class for embedding models.

    Any embedding model used with `Vectorstore` must implement this
    interface. The model is expected to convert input data (text, images, etc.)
    into a numerical vector representation.
    """


class EmbeddingModel(BaseEmbeddingModel[T], Generic[T]):
    def store(
        self,
        collection_name: str = "default",
        record_type: str = "ext::vectorstore::DefaultRecord",
    ) -> Vectorstore:
        return Vectorstore(
            embedding_model=self,
            collection_name=collection_name,
            record_type=record_type,
        )

    @abc.abstractmethod
    def generate(self, item: T) -> Vector:
        """
        Convert an input item into a list of floating-point values (vector
        embedding). Must be implemented in subclasses.
        """
        ...

    @abc.abstractmethod
    def generate_text(self, text: str) -> Vector: ...


@dataclasses.dataclass
class BaseVectorstore:
    """
    A framework-agnostic interface for interacting with Gel's ext::vectorstore.

    This class provides methods for storing, retrieving, and searching
    vector embeddings. It follows vector database conventions and supports
    different embedding models.
    """

    collection_name: str = "default"
    record_type: str = "ext::vectorstore::DefaultRecord"

    def add_embedding(
        self, embedding: Vector, text: Optional[str] = None, **metadata
    ) -> AddRecord:
        return AddRecord(
            record_type=self.record_type,
            collection_name=self.collection_name,
            embedding=embedding,
            text=text,
            metadata=metadata or None,
        )

    def delete(self, *ids: uuid.UUID) -> Query:
        """Delete records from the vector store by their IDs.

        Args:
            ids (List[uuid.UUID]): List of record IDs to delete.

        Returns:
            Query: Executable Query, returning the deleted IDs.
        """
        return Query(
            textwrap.dedent(
                f"""
                delete {quote.quote_ident(self.record_type)}
                    filter .id in array_unpack(<array<uuid>>$ids)
                    and .collection = <str>$collection_name;
                """
            ),
            collection_name=self.collection_name,
            ids=list(ids),
        )

    def get_by_ids(self, *ids: uuid.UUID) -> Query:
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
        return Query(
            textwrap.dedent(
                f"""
                select {self.record_type} {{
                    id,
                    text,
                    embedding,
                    metadata,
                }}
                filter .id in array_unpack(<array<uuid>>$ids)
                and .collection = <str>$collection_name;
                """
            ),
            collection_name=self.collection_name,
            ids=list(ids),
        )

    def search_by_vector(self, vector: Vector) -> SearchRecordQuery:
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
        return SearchRecordQuery(
            self.record_type, self.collection_name, vector
        )

    async def _update_record(self, id: uuid.UUID, **kwargs) -> Query:
        conditions = []
        params = {"id": id, "collection_name": self.collection_name}
        if "text" in kwargs:
            text = kwargs.pop("text")
            conditions.append("text := <str>$text")
            params["text"] = text
            if "embedding" not in kwargs:
                kwargs["embedding"] = await self._generate_vector_from_text(
                    text
                )
        if "embedding" in kwargs:
            conditions.append("embedding := <ext::pgvector::vector>$embedding")
            params["embedding"] = kwargs.pop("embedding")
        if "metadata" in kwargs:
            conditions.append("metadata := <json>$metadata")
            params["metadata"] = json.dumps(kwargs.pop("metadata"))
        if not conditions:
            raise errors.InterfaceError("No fields specified for update.")
        if kwargs:
            raise errors.InterfaceError(
                f"Unexpected fields for update: {', '.join(kwargs.keys())}"
            )
        return Query(
            textwrap.dedent(
                f"""
                update {quote.quote_ident(self.record_type)}
                    filter .id = <uuid>$id
                        and .collection = <str>$collection_name
                set {{
                    {", ".join(conditions)}
                }};
                """
            ),
            **params,
        )

    async def _generate_vector_from_text(self, text: str) -> Vector:
        raise NotImplementedError()


V = TypeVar("V")


def _iter_coroutine(coro: Coroutine[Any, Any, V]) -> V:
    try:
        coro.send(None)
    except StopIteration as ex:
        return ex.value
    finally:
        coro.close()


@dataclasses.dataclass
class Vectorstore(BaseVectorstore, Generic[T]):
    embedding_model: Optional[EmbeddingModel] = None

    async def _generate_vector_from_text(self, text: str) -> Vector:
        if self.embedding_model is None:
            raise errors.InterfaceError(
                "No embedding model provided to generate vector for text."
            )

        return self.embedding_model.generate_text(text)

    def add_text(self, text: str, **metadata) -> AddRecord:
        return AddRecord(
            record_type=self.record_type,
            collection_name=self.collection_name,
            embedding=_iter_coroutine(self._generate_vector_from_text(text)),
            text=text,
            metadata=metadata or None,
        )

    def search_by_item(self, item: T) -> SearchRecordQuery:
        """Search for similar items in the vector store.

        This method:
        1. Generates an embedding for the input item
        2. Finds records with similar embeddings
        3. Optionally filters results based on metadata
        4. Returns the most similar items up to the specified limit

        Args:
            item (Any): The query item to find similar matches for.
              Must be compatible with the embedding model's target_type.

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
        if self.embedding_model is None:
            raise errors.InterfaceError(
                "No embedding model provided to generate vector."
            )

        return SearchRecordQuery(
            self.record_type,
            self.collection_name,
            self.embedding_model.generate(item),
        )

    @overload
    def update_record(self, id: uuid.UUID, *, embedding: Vector) -> Query: ...

    @overload
    def update_record(self, id: uuid.UUID, *, text: str) -> Query: ...

    @overload
    def update_record(
        self, id: uuid.UUID, *, metadata: Optional[Dict[str, Any]]
    ) -> Query: ...

    @overload
    def update_record(
        self, id: uuid.UUID, *, text: str, embedding: Vector
    ) -> Query: ...

    @overload
    def update_record(
        self, id: uuid.UUID, *, text: str, metadata: Optional[Dict[str, Any]]
    ) -> Query: ...

    @overload
    def update_record(
        self,
        id: uuid.UUID,
        *,
        embedding: Vector,
        metadata: Optional[Dict[str, Any]],
    ) -> Query: ...

    @overload
    def update_record(
        self,
        id: uuid.UUID,
        *,
        text: str,
        embedding: Vector,
        metadata: Optional[Dict[str, Any]],
    ) -> Query: ...

    def update_record(self, id: uuid.UUID, **kwargs) -> Query:
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
        return _iter_coroutine(self._update_record(id, **kwargs))


class AsyncEmbeddingModel(BaseEmbeddingModel, Generic[T]):
    """
    Abstract base class for embedding models.

    Any embedding model used with `Vectorstore` must implement this
    interface. The model is expected to convert input data (text, images, etc.)
    into a numerical vector representation.
    """

    def store(
        self,
        collection_name: str = "default",
        record_type: str = "ext::vectorstore::DefaultRecord",
    ) -> AsyncVectorstore:
        return AsyncVectorstore(
            embedding_model=self,
            collection_name=collection_name,
            record_type=record_type,
        )

    @abc.abstractmethod
    async def generate(self, item: T) -> Vector:
        """
        Convert an input item into a list of floating-point values (vector
        embedding). Must be implemented in subclasses.
        """
        ...

    @abc.abstractmethod
    async def generate_text(self, text: str) -> Vector: ...


@dataclasses.dataclass
class AsyncVectorstore(BaseVectorstore, Generic[T]):
    embedding_model: Optional[AsyncEmbeddingModel] = None

    async def add_text(self, text: str, **metadata) -> AddRecord:
        if self.embedding_model is None:
            raise errors.InterfaceError(
                "No embedding model provided to generate vector for text."
            )

        return AddRecord(
            record_type=self.record_type,
            collection_name=self.collection_name,
            embedding=await self.embedding_model.generate_text(text),
            text=text,
            metadata=metadata or None,
        )

    async def search_by_item(self, item: str) -> SearchRecordQuery:
        """Search for similar items in the vector store.

        This method:
        1. Generates an embedding for the input item
        2. Finds records with similar embeddings
        3. Optionally filters results based on metadata
        4. Returns the most similar items up to the specified limit

        Args:
            item (Any): The query item to find similar matches for.
              Must be compatible with the embedding model's target_type.

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
        if self.embedding_model is None:
            raise errors.InterfaceError(
                "No embedding model provided to generate vector."
            )

        return SearchRecordQuery(
            self.record_type,
            self.collection_name,
            await self.embedding_model.generate(item),
        )

    @overload
    async def update_record(
        self, id: uuid.UUID, *, embedding: Vector
    ) -> Query: ...

    @overload
    async def update_record(self, id: uuid.UUID, *, text: str) -> Query: ...

    @overload
    async def update_record(
        self, id: uuid.UUID, *, metadata: Optional[Dict[str, Any]]
    ) -> Query: ...

    @overload
    async def update_record(
        self, id: uuid.UUID, *, text: str, embedding: Vector
    ) -> Query: ...

    @overload
    async def update_record(
        self, id: uuid.UUID, *, text: str, metadata: Optional[Dict[str, Any]]
    ) -> Query: ...

    @overload
    async def update_record(
        self,
        id: uuid.UUID,
        *,
        embedding: Vector,
        metadata: Optional[Dict[str, Any]],
    ) -> Query: ...

    @overload
    async def update_record(
        self,
        id: uuid.UUID,
        *,
        text: str,
        embedding: Vector,
        metadata: Optional[Dict[str, Any]],
    ) -> Query: ...

    async def update_record(self, id: uuid.UUID, **kwargs) -> Query:
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
        return await self._update_record(id, **kwargs)
