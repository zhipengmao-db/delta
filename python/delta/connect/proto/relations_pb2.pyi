#
# Copyright (2024) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file

Copyright (2024) The Delta Lake Project Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import builtins
import delta.connect.proto.proto.base_pb2
import google.protobuf.descriptor
import google.protobuf.message
import sys

if sys.version_info >= (3, 8):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class DeltaRelation(google.protobuf.message.Message):
    """Message to hold all relation extensions in Delta Connect."""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SCAN_FIELD_NUMBER: builtins.int
    @property
    def scan(self) -> global___Scan: ...
    def __init__(
        self,
        *,
        scan: global___Scan | None = ...,
    ) -> None: ...
    def HasField(
        self,
        field_name: typing_extensions.Literal["relation_type", b"relation_type", "scan", b"scan"],
    ) -> builtins.bool: ...
    def ClearField(
        self,
        field_name: typing_extensions.Literal["relation_type", b"relation_type", "scan", b"scan"],
    ) -> None: ...
    def WhichOneof(
        self, oneof_group: typing_extensions.Literal["relation_type", b"relation_type"]
    ) -> typing_extensions.Literal["scan"] | None: ...

global___DeltaRelation = DeltaRelation

class Scan(google.protobuf.message.Message):
    """Relation that reads from a Delta table."""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TABLE_FIELD_NUMBER: builtins.int
    @property
    def table(self) -> delta.connect.proto.base_pb2.DeltaTable:
        """(Required) The Delta table to scan."""
    def __init__(
        self,
        *,
        table: delta.connect.proto.base_pb2.DeltaTable | None = ...,
    ) -> None: ...
    def HasField(
        self, field_name: typing_extensions.Literal["table", b"table"]
    ) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["table", b"table"]) -> None: ...

global___Scan = Scan