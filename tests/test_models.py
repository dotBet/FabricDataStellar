from dataclasses import replace

import pytest

from data_stellar import (
    Column,
    DataType,
    Dimension,
    Mart,
    ModelError,
    Reference,
    Table,
)


def table(name: str = "silver.projects", **kwargs: object) -> Table:
    base = Table(
        name=name,
        columns=(Column("id", DataType.TEXT),),
        grain=("id",),
        query="SELECT 'P100' AS id",
    )
    return replace(base, **kwargs)


def test_dependency_order_is_not_declaration_order() -> None:
    source = table("bronze.projects")
    target = table(depends_on=(source.name,))
    assert Mart("example", (target, source)).plan() == (source, target)


def test_invalid_grain_is_rejected() -> None:
    with pytest.raises(ModelError, match="unknown columns"):
        table(grain=("missing",))
    with pytest.raises(ModelError, match="cannot be nullable"):
        table(columns=(Column("id", DataType.TEXT, nullable=True),))


def test_unknown_dependencies_duplicates_and_cycles_are_rejected() -> None:
    source = table()
    with pytest.raises(ModelError, match="unknown dependency"):
        Mart("example", (table(depends_on=("bronze.missing",)),)).plan()
    with pytest.raises(ModelError, match="uniquely named"):
        Mart("example", (source, source)).plan()
    with pytest.raises(ModelError, match="Dependency cycle"):
        Mart(
            "example",
            (
                table("silver.one", depends_on=("silver.two",)),
                table("silver.two", depends_on=("silver.one",)),
            ),
        ).plan()


def test_reference_must_have_a_dependency_and_unique_target() -> None:
    reference = Reference(("id",), "silver.parent", ("label",))
    with pytest.raises(ModelError, match="explicit dependency"):
        table(references=(reference,))
    parent = Table(
        name="silver.parent",
        columns=(Column("id", DataType.TEXT), Column("label", DataType.TEXT)),
        grain=("id",),
        query="SELECT '1' AS id, 'name' AS label",
    )
    child = table(depends_on=(parent.name,), references=(reference,))
    with pytest.raises(ModelError, match="declared unique key"):
        Mart("example", (parent, child)).plan()


def test_dimension_surrogate_must_define_its_grain() -> None:
    with pytest.raises(ModelError, match="surrogate key"):
        Dimension(
            name="gold.dim_project",
            columns=(Column("id", DataType.TEXT), Column("project_key", DataType.TEXT)),
            grain=("id",),
            business_key=("id",),
            surrogate_key="project_key",
            query="SELECT '1' AS id, 'key' AS project_key",
        )


@pytest.mark.parametrize("name", ["projects", "gold.project;drop", "_stellar.projects"])
def test_unsafe_or_reserved_names_are_rejected(name: str) -> None:
    with pytest.raises(ModelError):
        table(name)
