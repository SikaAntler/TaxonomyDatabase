from pathlib import Path

import pytest
from taxonomy import Taxonomy

filename = Path("../databases/test.db")
taxa = ("phylum", "class", "order", "family", "genus", "species")
test_data = {
    "phylum": ("phylum", "蓝藻门", "Cyanophyta", "", None),
    "class": ("class", "蓝藻纲", "Cyanophyceae", "", "蓝藻门"),
    "order": ("order", "色球藻目", "Chroococcales", "", "蓝藻纲"),
    "family": ("family", "微囊藻科", "Microcystaceae", "", "色球藻目"),
    "genus": ("genus", "微囊藻属", "Microcystis", "", "微囊藻科"),
    "species": (
        ("species", "铜绿微囊藻", "Microcystis aeruginosa", "", "微囊藻属"),
        ("species", "水华微囊藻", "Microcystis flosaquae", "", "微囊藻属"),
    ),
}


@pytest.mark.dependency(name="init")
class TestInit:
    """测试无数据库文件时初始化是否成功。"""

    @pytest.mark.dependency(name="create_tables")
    def test_create_tables(self):
        if filename.exists():
            filename.unlink(missing_ok=True)
        database = Taxonomy(filename)

        database._execute(
            """
select name
from sqlite_master
where type = 'table';
"""
        )
        items = database._fetchall()
        assert len(items) == 6
        for i in range(6):
            assert items[i][0] == taxa[i]

    @pytest.mark.dependency(depends=["create_tables"])
    def test_create_triggers(self):
        database = Taxonomy(filename)
        database._execute(
            """
select name
from sqlite_master
where type = 'trigger';
            """
        )
        items = database._fetchall()
        assert len(items) == 10
        for i in range(5):
            assert (
                items[2 * i][0] == f"update_{taxa[i]}_chinese"
                and items[2 * i + 1][0] == f"delete_{taxa[i]}"
            )

        database.close()


@pytest.mark.dependency(name="create", depends=["init"])
class TestCreate:
    @pytest.mark.dependency(name="create_phylum")
    def test_create_phylum(self):
        self._test_template(test_data["phylum"], 3)

    @pytest.mark.dependency(name="create_class", depends=["create_phylum"])
    def test_create_class(self):
        self._test_template(test_data["class"], 4)

    @pytest.mark.dependency(name="create_order", depends=["create_class"])
    def test_create_order(self):
        self._test_template(test_data["order"], 4)

    @pytest.mark.dependency(name="create_family", depends=["create_order"])
    def test_create_family(self):
        self._test_template(test_data["family"], 4)

    @pytest.mark.dependency(name="create_genus", depends=["create_family"])
    def test_create_genus(self):
        self._test_template(test_data["genus"], 4)

    @pytest.mark.dependency(name="create_species", depends=["create_genus"])
    def test_create_species(self):
        self._test_template(test_data["species"][0], 4)
        self._test_template(test_data["species"][1], 4)

    @staticmethod
    def _test_template(data: tuple, length: int):
        database = Taxonomy(filename)
        database.create_data(*data)
        database.commit()

        item = database.retrieve_data_by_chinese(data[0], data[1])
        assert len(item) == length
        for i in range(length):
            assert item[i] == data[i + 1]


@pytest.mark.dependency(name="retrieve", depends=["create"])
class TestRetrieve:
    @pytest.mark.dependency()
    def test_retrieve_date_by_child(self):
        database = Taxonomy(filename)
        item = database.retrieve_data_by_child("class", "蓝藻纲")
        self._valid(test_data["phylum"], item, 3)

    @pytest.mark.dependency()
    def test_retrieve_data_by_chinese(self):
        database = Taxonomy(filename)
        item = database.retrieve_data_by_chinese("class", "蓝藻纲")
        self._valid(test_data["class"], item, 4)

    @pytest.mark.dependency()
    def test_retrieve_data_by_parent(self):
        database = Taxonomy(filename)
        items = database.retrieve_data_by_parent("phylum", None)
        assert len(items) == 1
        self._valid(test_data["phylum"][:-1], items[0], 3)
        items = database.retrieve_data_by_parent("species", "微囊藻属")
        assert len(items) == 2
        self._valid(test_data["species"][0], items[0], 4)
        self._valid(test_data["species"][1], items[1], 4)

    @pytest.mark.dependency()
    def test_retrieve_data_by_taxon(self):
        database = Taxonomy(filename)
        items = database.retrieve_data_by_taxon("species")
        assert len(items) == 2
        self._valid(test_data["species"][0], items[0], 4)
        self._valid(test_data["species"][1], items[1], 4)

    @pytest.mark.dependency()
    def test_retrieve_hierarchical_parents_by_child(self):
        database = Taxonomy(filename)
        items = database.retrieve_hierarchical_parents_by_child("class", "蓝藻纲")
        assert len(items) == 1 and items[0] == test_data["phylum"][1:-1]

        items = database.retrieve_hierarchical_parents_by_child(
            "genus", "微囊藻属", True, True
        )
        assert (
            len(items) == 5
            and items[0] == test_data["phylum"][1:3]
            and items[1] == test_data["class"][1:3]
            and items[2] == test_data["order"][1:3]
            and items[3] == test_data["family"][1:3]
            and items[4] == test_data["genus"][1:3]
        )

    @staticmethod
    def _valid(data: tuple, item, length: int):
        assert len(item) == length
        for i in range(length):
            assert item[i] == data[i + 1]


@pytest.mark.dependency(name="update", depends=["create"])
class TestUpdate:
    @pytest.mark.dependency()
    def test_update_chinese(self):
        database = Taxonomy(filename)
        database.update_chinese("species", "铜绿微囊藻", "微囊藻")
        item = database.retrieve_data_by_chinese("species", "微囊藻")
        assert item is not None

    @pytest.mark.dependency()
    def test_update_scientific(self):
        database = Taxonomy(filename)
        database.update_scientific("species", "水华微囊藻", "LOL")
        item = database.retrieve_data_by_chinese("species", "水华微囊藻")
        assert item[1] == "LOL"


@pytest.mark.dependency(name="delete", depends=["create"])
class TestDelete:
    @pytest.mark.dependency()
    def test_delete_date(self):
        database = Taxonomy(filename)
        database.delete_data("phylum", "蓝藻门")
        for taxon in taxa:
            items = database.retrieve_data_by_taxon(taxon)
            assert items == []
