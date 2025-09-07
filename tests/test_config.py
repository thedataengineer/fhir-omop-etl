import pytest

from textwrap import dedent

from fhiromop.config import FieldMapping, load_mapping


def test_load_mapping_valid(tmp_path):
    content = dedent(
        """
        person_id:
          source: id
        gender_concept_id:
          default: 0
        """
    )
    path = tmp_path / "cfg.yaml"
    path.write_text(content)
    cfg = load_mapping(path)
    assert "person_id" in cfg.fields
    assert isinstance(cfg.fields["gender_concept_id"], FieldMapping)
    assert cfg.fields["gender_concept_id"].default == 0


def test_load_mapping_invalid_root(tmp_path):
    path = tmp_path / "cfg.yaml"
    path.write_text("- not_a_mapping")
    with pytest.raises(ValueError):
        load_mapping(path)


def test_load_mapping_requires_source_or_default(tmp_path):
    path = tmp_path / "cfg.yaml"
    path.write_text("field: {}")
    with pytest.raises(ValueError):
        load_mapping(path)


def test_field_mapping_defaults():
    fm = FieldMapping()
    assert fm.source is None
    assert fm.default is None
