#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


import asyncio
import pathlib
import shutil
import os
import tempfile

from edgedb import _testbase as tb


class TestCodegen(tb.AsyncQueryTestCase):
    async def test_codegen(self):
        env = os.environ.copy()
        env.update(
            {
                f"EDGEDB_{k.upper()}": str(v)
                for k, v in self.get_connect_args().items()
            }
        )
        container = pathlib.Path(__file__).absolute().parent / "codegen"
        with tempfile.TemporaryDirectory() as td:
            td_path = pathlib.Path(td)
            for project in container.iterdir():
                cwd = td_path / project.name
                shutil.copytree(project, cwd)
                await self._test_codegen(env, cwd)

    async def _test_codegen(self, env, cwd: pathlib.Path):
        async def run(*args, extra_env=None):
            if extra_env is None:
                env_ = env
            else:
                env_ = env.copy()
                env_.update(extra_env)
            p = await asyncio.create_subprocess_exec(
                *args,
                cwd=cwd,
                env=env_,
            )
            try:
                await asyncio.wait_for(p.wait(), 10)
            except asyncio.TimeoutError:
                p.terminate()
                await p.wait()
                raise

        await run(
            "edgedb-py", extra_env={"EDGEDB_PYTHON_CODEGEN_PY_VER": "3.8.5"}
        )
        await run(
            "edgedb-py",
            "--target",
            "blocking",
            extra_env={"EDGEDB_PYTHON_CODEGEN_PY_VER": "3.9.2"},
        )
        await run(
            "edgedb-py",
            "--target",
            "async",
            "--file",
            extra_env={"EDGEDB_PYTHON_CODEGEN_PY_VER": "3.10.3"},
        )

        for f in cwd.rglob("*.py"):
            a = f.with_suffix(".py.assert")
            self.assertEqual(f.read_text(), a.read_text())
        for a in cwd.rglob("*.py.assert"):
            f = a.with_suffix("")
            self.assertTrue(f.exists(), f"{f} doesn't exist")
