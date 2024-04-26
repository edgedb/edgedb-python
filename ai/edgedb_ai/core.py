#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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

from __future__ import annotations
import typing

import edgedb
import httpx
import httpx_sse

from . import types


def create_AI(client: edgedb.Client, **kwargs) -> EdgeDBAI:
    client.ensure_connected()
    return EdgeDBAI(client, types.AIOptions(**kwargs))


async def create_async_AI(
    client: edgedb.AsyncIOClient, **kwargs
) -> AsyncEdgeDBAI:
    await client.ensure_connected()
    return AsyncEdgeDBAI(client, types.AIOptions(**kwargs))


class BaseEdgeDBAI:
    options: types.AIOptions
    context: types.QueryContext
    client_cls = NotImplemented

    def __init__(
        self,
        client: typing.Union[edgedb.Client, edgedb.AsyncIOClient],
        options: types.AIOptions,
        **kwargs,
    ):
        pool = getattr(client, "_impl")
        host, port = getattr(pool, "_working_addr")
        params = getattr(pool, "_working_params")
        proto = "http" if params.tls_security == "insecure" else "https"
        branch = params.branch
        self.options = options
        self.context = types.QueryContext(**kwargs)
        self._init_client(
            base_url=f"{proto}://{host}:{port}/branch/{branch}/ext/ai",
            auth=(params.user, params.password),
            verify=params.ssl_ctx,
        )

    def _init_client(self, **kwargs):
        raise NotImplementedError

    def with_config(self, **kwargs) -> typing.Self:
        cls = type(self)
        rv = cls.__new__(cls)
        rv.options = self.options.derive(kwargs)
        rv.context = self.context
        rv.client = self.client
        return rv

    def with_context(self, **kwargs) -> typing.Self:
        cls = type(self)
        rv = cls.__new__(cls)
        rv.options = self.options
        rv.context = self.context.derive(kwargs)
        rv.client = self.client
        return rv


class EdgeDBAI(BaseEdgeDBAI):
    client: httpx.Client

    def _init_client(self, **kwargs):
        self.client = httpx.Client(**kwargs)

    def query_rag(
        self, message: str, context: typing.Optional[types.QueryContext] = None
    ) -> str:
        if context is None:
            context = self.context
        resp = self.client.post(
            **types.RAGRequest(
                model=self.options.model,
                prompt=self.options.prompt,
                context=context,
                query=message,
                stream=False,
            ).to_httpx_request()
        )
        resp.raise_for_status()
        return resp.json()["response"]

    def stream_rag(
        self, message: str, context: typing.Optional[types.QueryContext] = None
    ):
        if context is None:
            context = self.context
        with httpx_sse.connect_sse(
            self.client,
            "post",
            **types.RAGRequest(
                model=self.options.model,
                prompt=self.options.prompt,
                context=context,
                query=message,
                stream=True,
            ).to_httpx_request(),
        ) as event_source:
            event_source.response.raise_for_status()
            for sse in event_source.iter_sse():
                yield sse.data


class AsyncEdgeDBAI(BaseEdgeDBAI):
    client: httpx.AsyncClient

    def _init_client(self, **kwargs):
        self.client = httpx.AsyncClient(**kwargs)

    async def query_rag(
        self, message: str, context: typing.Optional[types.QueryContext] = None
    ) -> str:
        if context is None:
            context = self.context
        resp = await self.client.post(
            **types.RAGRequest(
                model=self.options.model,
                prompt=self.options.prompt,
                context=context,
                query=message,
                stream=False,
            ).to_httpx_request()
        )
        resp.raise_for_status()
        return resp.json()["response"]

    async def stream_rag(
        self, message: str, context: typing.Optional[types.QueryContext] = None
    ):
        if context is None:
            context = self.context
        async with httpx_sse.aconnect_sse(
            self.client,
            "post",
            **types.RAGRequest(
                model=self.options.model,
                prompt=self.options.prompt,
                context=context,
                query=message,
                stream=True,
            ).to_httpx_request(),
        ) as event_source:
            event_source.response.raise_for_status()
            async for sse in event_source.aiter_sse():
                yield sse.data
