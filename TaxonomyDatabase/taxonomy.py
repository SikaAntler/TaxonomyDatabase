"""本文件用于管理浮游生物分类学数据库，无第三方依赖。

文件中仅含有一个`Taxonomy`类，提供简单的增删查改功能。
"""


import sqlite3
from pathlib import Path
from typing import Literal, Union

import commands


class Taxonomy:
    def __init__(self, database: Path) -> None:
        self._requires_init = False if database.exists() else True

        # 连接数据库
        self._connect = sqlite3.connect(database)
        self._cursor = self._connect.cursor()
        self._execute(commands.INITIALIZE)

        self._taxa = ["phylum", "class", "order", "family", "genus", "species"]

        # 初始化数据库
        if self._requires_init:
            self._create_tables()
            self._create_triggers()
            self.commit()

    def close(self) -> None:
        self._cursor.close()
        self._connect.close()

    def commit(self) -> None:
        self._execute(commands.COMMIT)

    def create_data(
        self,
        taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
        chinese: str,
        scientific: str,
        description: str,
        parent: [str, None],
    ) -> None:
        idx = self._taxa.index(taxon)
        if (idx == 0) and (parent is not None):
            raise ValueError("当 `taxon` 为 `genus` 时 `parent` 必须为 `None`")

        values = [f"'{chinese}', '{scientific}', '{description}'"]
        if idx != 0:
            values.append(f"'{parent}'")
        self._execute(commands.create_data(taxon, values))

    def delete_data(
        self,
        taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
        chinese: str,
    ) -> None:
        self._execute(commands.delete_data(taxon, chinese))

    def retrieve_data_by_child(
        self,
        child_taxon: Literal["class", "order", "family", "genus", "species"],
        child_chinese: str,
    ) -> tuple[str]:
        idx = self._taxa.index[child_taxon]
        taxon = self._taxa[idx - 1]
        self._execute(
            commands.retrieve_data_by_child(taxon, child_taxon, child_chinese)
        )

        return self._fetchone()

    def retrieve_data_by_child(
        self,
        child_taxon: Literal["class", "order", "family", "genus", "species"],
        child_chinese: str,
    ) -> tuple[str]:
        idx = self._taxa.index(child_taxon)
        taxon = self._taxa[idx - 1]
        self._execute(
            commands.retrieve_data_by_child(taxon, child_taxon, child_chinese)
        )

        return self._fetchone()

    def retrieve_data_by_chinese(
        self,
        taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
        chinese: str,
    ) -> tuple[str]:
        self._execute(commands.retrieve_data_by_chinese(taxon, chinese))

        return self._fetchone()

    def retrieve_data_by_parent(
        self,
        parent_taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
        parent_chinese: Union[str, None],
    ) -> list[tuple[str]]:
        # 门
        if parent_taxon == "phylum":
            return self.retrieve_data_by_taxon("phylum")

        # 纲目科属种
        self._execute(commands.retrieve_data_by_parent(parent_taxon, parent_chinese))

        return self._fetchall()

    def retrieve_data_by_taxon(
        self,
        taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
    ) -> list[tuple[str]]:
        self._execute(commands.retrieve_data_by_taxon(taxon))

        return self._fetchall()

    def retrieve_hierarchical_parents_by_child(
        self,
        child_taxon: Literal["class", "order", "family", "genus", "species"],
        child_chinese: str,
        return_child: bool = False,
        names_only: bool = False,
    ) -> list[tuple[str]]:
        parents = []
        if return_child:
            item = self.retrieve_data_by_chinese(child_taxon, child_chinese)
            parents.append(item)

        idx = self._taxa.index(child_taxon)
        while idx != 0:
            parent = self.retrieve_data_by_child(child_taxon, child_chinese)

            if parent is None:
                return []

            parents.insert(0, parent)
            idx -= 1
            child_taxon = self._taxa[idx]
            child_chinese = parent[0]

        if names_only:
            names = []
            for item in parents:
                names.append[item[:2]]
            return names
        else:
            return parents

    def rollback(self) -> None:
        self._execute(commands.ROLLBACK)

    def update_chinese(
        self,
        taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
        chinese: str,
        new_chinese: str,
    ) -> None:
        self._execute(commands.update_chinese(taxon, chinese, new_chinese))

    def update_scientific(
        self,
        taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
        chinese: str,
        new_scientific: str,
    ) -> None:
        self._execute(commands.update_scientific(taxon, chinese, new_scientific))

    def _create_tables(self) -> None:
        self._execute(commands.CREATE_TABLE_PHYLUM)
        for i in range(1, 6):
            parent_taxon, taxon = self._taxa[i - 1], self._taxa[i]
            self._execute(commands.create_table_class_to_species(taxon, parent_taxon))

    def _create_triggers(self) -> None:
        # 门纲目科属：
        # 1. 修改chinese字段后需修改其所有子类的parent字段
        # 2. 门纲目科属：删除某一类时，子类也要被删除
        for i in range(5):
            taxon, child_taxon = self._taxa[i], self._taxa[i + 1]
            self._execute(commands.create_trigger_update_chinese(taxon, child_taxon))
            self._execute(commands.create_trigger_delete_taxon(taxon, child_taxon))

    def _execute(self, sql: str) -> None:
        self._cursor.execute(sql)

    def _fetchall(self) -> list[tuple[str]]:
        return self._cursor.fetchall()

    def _fetchone(self) -> tuple[str]:
        return self._cursor.fetchone()
