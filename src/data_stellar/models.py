from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from graphlib import CycleError, TopologicalSorter


class ModelError(ValueError):
    """A model definition violates a modeling contract."""


def identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ModelError(f"Invalid identifier: {value!r}")
    return value


def qualified_name(value: str) -> tuple[str, str]:
    parts = value.split(".")
    if len(parts) != 2:
        raise ModelError(f"Expected schema.table, got {value!r}")
    schema, table = (identifier(part) for part in parts)
    if schema.startswith("_stellar"):
        raise ModelError("The _stellar schema prefix is reserved for runtime metadata")
    return schema, table


class DataType(StrEnum):
    TEXT = "VARCHAR"
    INTEGER = "BIGINT"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMPTZ"
    AMOUNT = "DECIMAL(18, 2)"
    WEIGHT = "DECIMAL(18, 8)"

    @property
    def numeric(self) -> bool:
        return self in (DataType.INTEGER, DataType.AMOUNT, DataType.WEIGHT)


class Aggregation(StrEnum):
    SUM = "sum"
    LAST = "last"
    NONE = "none"


@dataclass(frozen=True)
class Column:
    name: str
    data_type: DataType
    nullable: bool = False

    def __post_init__(self) -> None:
        identifier(self.name)
        if not isinstance(self.data_type, DataType):
            raise ModelError(f"{self.name}: data_type must be a DataType")


@dataclass(frozen=True)
class Reference:
    columns: tuple[str, ...]
    target: str
    target_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        qualified_name(self.target)
        if not self.columns or len(self.columns) != len(self.target_columns):
            raise ModelError("References require equally sized, non-empty column tuples")
        for names in (self.columns, self.target_columns):
            if len(set(names)) != len(names):
                raise ModelError("Reference columns must not repeat")
            for name in names:
                identifier(name)


@dataclass(frozen=True)
class Check:
    name: str
    predicate: str

    def __post_init__(self) -> None:
        identifier(self.name)
        if not self.predicate.strip():
            raise ModelError(f"{self.name}: check predicate must not be empty")


@dataclass(frozen=True)
class Measure:
    column: str
    aggregation: Aggregation = Aggregation.SUM
    currency_column: str | None = None

    def __post_init__(self) -> None:
        identifier(self.column)
        if not isinstance(self.aggregation, Aggregation):
            raise ModelError(f"{self.column}: aggregation must be an Aggregation")
        if self.currency_column is not None:
            identifier(self.currency_column)


@dataclass(frozen=True)
class Allocation:
    weight_column: str
    group_by: tuple[str, ...]


@dataclass(frozen=True, kw_only=True)
class Table:
    name: str
    columns: tuple[Column, ...]
    grain: tuple[str, ...]
    query: str
    depends_on: tuple[str, ...] = ()
    references: tuple[Reference, ...] = ()
    checks: tuple[Check, ...] = ()

    def __post_init__(self) -> None:
        qualified_name(self.name)
        names = [column.name for column in self.columns]
        if not names or len(set(names)) != len(names):
            raise ModelError(f"{self.name}: columns must be non-empty and unique")
        self.require_key(self.grain)
        if not self.query.strip():
            raise ModelError(f"{self.name}: query must not be empty")
        if len(set(self.depends_on)) != len(self.depends_on):
            raise ModelError(f"{self.name}: repeated dependency")
        for dependency in self.depends_on:
            qualified_name(dependency)
        for reference in self.references:
            self.require_columns(reference.columns)
            if reference.target not in self.depends_on:
                raise ModelError(f"{self.name}: reference target must be an explicit dependency")
        check_names = [check.name for check in self.checks]
        if len(set(check_names)) != len(check_names):
            raise ModelError(f"{self.name}: repeated check name")

    def require_columns(self, names: tuple[str, ...]) -> None:
        missing = set(names) - {column.name for column in self.columns}
        if missing:
            raise ModelError(f"{self.name}: unknown columns {sorted(missing)}")

    def require_key(self, names: tuple[str, ...]) -> None:
        if not names or len(set(names)) != len(names):
            raise ModelError(f"{self.name}: keys must be non-empty and unique")
        self.require_columns(names)
        nullable = {column.name for column in self.columns if column.nullable}
        if nullable.intersection(names):
            raise ModelError(f"{self.name}: key columns cannot be nullable")


@dataclass(frozen=True, kw_only=True)
class Dimension(Table):
    business_key: tuple[str, ...]
    surrogate_key: str

    def __post_init__(self) -> None:
        super().__post_init__()
        self.require_key(self.business_key)
        if self.grain != (self.surrogate_key,):
            raise ModelError(f"{self.name}: dimension grain must be its surrogate key")


@dataclass(frozen=True, kw_only=True)
class Fact(Table):
    measures: tuple[Measure, ...]

    def __post_init__(self) -> None:
        super().__post_init__()
        columns = {column.name: column for column in self.columns}
        measure_names = [measure.column for measure in self.measures]
        if len(set(measure_names)) != len(measure_names):
            raise ModelError(f"{self.name}: repeated measure")
        for measure in self.measures:
            self.require_columns((measure.column,))
            if not columns[measure.column].data_type.numeric:
                raise ModelError(f"{self.name}: measures must be numeric")
            if measure.currency_column is not None:
                self.require_columns((measure.currency_column,))
                currency = columns[measure.currency_column]
                if currency.data_type != DataType.TEXT or currency.nullable:
                    raise ModelError(f"{self.name}: currency must be non-null text")


@dataclass(frozen=True, kw_only=True)
class Bridge(Table):
    allocation: Allocation | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if len(self.references) < 2:
            raise ModelError(f"{self.name}: a bridge requires at least two references")
        if self.allocation is not None:
            self.require_key(self.allocation.group_by)
            self.require_columns((self.allocation.weight_column,))
            weight = next(c for c in self.columns if c.name == self.allocation.weight_column)
            if weight.data_type != DataType.WEIGHT or weight.nullable:
                raise ModelError(f"{self.name}: allocation weight must be non-null WEIGHT")
            if not set(self.allocation.group_by) < set(self.grain):
                raise ModelError(f"{self.name}: allocation group must be a proper subset of grain")


@dataclass(frozen=True)
class Mart:
    name: str
    models: tuple[Table, ...]

    def plan(self) -> tuple[Table, ...]:
        identifier(self.name)
        registry = {model.name: model for model in self.models}
        if not registry or len(registry) != len(self.models):
            raise ModelError("A mart requires non-empty, uniquely named models")
        for model in self.models:
            for dependency in model.depends_on:
                if dependency not in registry:
                    raise ModelError(f"{model.name}: unknown dependency {dependency}")
            for reference in model.references:
                target = registry[reference.target]
                target.require_columns(reference.target_columns)
                keys = [target.grain]
                if isinstance(target, Dimension):
                    keys.append(target.business_key)
                if reference.target_columns not in keys:
                    raise ModelError(
                        f"{model.name}: reference target must be a declared unique key"
                    )
                if isinstance(model, Fact) and not isinstance(target, Dimension):
                    raise ModelError(f"{model.name}: fact references must target dimensions")
        graph = {model.name: model.depends_on for model in self.models}
        try:
            order = tuple(TopologicalSorter(graph).static_order())
        except CycleError as error:
            raise ModelError(f"Dependency cycle: {error.args[1]}") from error
        return tuple(registry[name] for name in order)
