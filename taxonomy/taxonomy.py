"""本文件中仅含有一个`Taxonomy`类，提供简单的增删查改功能。
"""


import sqlite3
from pathlib import Path
from typing import Literal, Union

from . import commands


class Taxonomy:
    """连接和操作浮游生物数据库的类。

    使用SQLite3作为底层数据库，将SQL语句封装成函数供其他Python脚本调用。

    Examples:
        ```python
        from taxonomy import Taxonomy

        database = Taxonomy("databases/phytoplankton.db")
        ```
    即可完成初始化，如果目标文件不存在，会自动新建包含所有表和触发器的"空白"数据库。

    函数命名规则:
        - **taxon(taxa)：**类别，指门、纲、目、科、属、种六类中的一个；
        - **data：**记录，指数据库表中中一条数据；
        - **retrieve_data_by_xxx：**其中**xxx**如果没有特别说明，默认指的是中文名。

    !!! warning "注意"
        类中以下划线开头的属性和类均代表不希望被访问。
    """

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
        """关闭数据库。

        不需要使用数据库时调用，如在Qt中触发`closeEvent`时。
        """
        self._cursor.close()
        self._connect.close()

    def commit(self) -> None:
        """提交改动。

        确认提交时调用，如使用增加、删除、修改等函数后。

        !!! warning "注意"
            类中的所有增删查改函数均不会调用此函数，需使用者手动调用。
        """
        self._connect.commit()

    def create_data(
        self,
        taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
        chinese: str,
        scientific: str,
        description: str,
        parent: Union[str, None],
    ) -> None:
        """插入一条生物的类别信息。

        Args:
            taxon: 生物的类别。
            chinese: 生物的中文名。
            scientific: 生物的学名。
            description: 生物的长文字描述。
            parent: 生物父类的中文名，门没有父类填None。
        """
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
        """给定生物的类别和中文名删除其数据。

        Args:
            taxon: 生物的类别。
            chinese: 生物的中文名。
        """
        self._execute(commands.delete_data(taxon, chinese))

    def retrieve_data_by_child(
        self,
        child_taxon: Literal["class", "order", "family", "genus", "species"],
        child_chinese: str,
    ) -> tuple:
        """给定生物的类别和中文名返回其父类的全部信息。

        Args:
            child_taxon: 生物的类别。
            child_chinese: 生物的中文名。

        Returns:
            生物父类的全部信息。
        """
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
    ) -> tuple:
        """给定生物的类别和中文名返回该生物的全部信息。

        Args:
            taxon: 生物的类别。
            chinese: 生物的中文名。

        Returns:
            该生物的全部信息。

        Examples:
            ```python
            from taxonomy import Taxonomy

            database = Taxonomy("databases/phytoplankton.db")
            phylum_info = database.get_record_by_chinese("genus", "角甲藻属")

            >>> phylum_info
            ('角甲藻属', 'Ceratium', '无', 0.0, 0.0, 100.0, 100.0, '角甲藻科')
            ```
        """
        self._execute(commands.retrieve_data_by_chinese(taxon, chinese))

        return self._fetchone()

    def retrieve_data_by_parent(
        self,
        taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
        parent_chinese: Union[str, None],
    ) -> list[tuple]:
        """给定生物的类别和父类的中文名返回该类别所有生物的信息。

        Args:
            taxon: 生物的类别。
            parent_chinese: 父类的中文名，门没有父类填None。

        Returns:
            该生物的全部信息。
        """
        # 门
        if taxon == "phylum":
            return self.retrieve_data_by_taxon("phylum")

        # 纲目科属种
        self._execute(commands.retrieve_data_by_parent(taxon, parent_chinese))

        return self._fetchall()

    def retrieve_data_by_taxon(
        self,
        taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
    ) -> list[tuple]:
        """查询门、纲、目、科、属、种六类中某一类别所有生物的信息。

        Args:
            taxon: 生物的类别。

        Returns:
            类别中包含的所有生物的信息。
        """
        self._execute(commands.retrieve_data_by_taxon(taxon))

        return self._fetchall()

    def retrieve_hierarchical_parents_by_child(
        self,
        child_taxon: Literal["class", "order", "family", "genus", "species"],
        child_chinese: str,
        return_child: bool = False,
        names_only: bool = False,
    ) -> list[tuple]:
        """给定某个生物的类别和中文名返回其所有父类的信息。

        Args:
            child_taxon: 生物的类别。
            child_chinese: 生物的中文名。
            return_child: 返回值是否包含自身。
            names_only: 返回值是否仅包含中文名和学名。

        Returns:
            生物所有父类的信息。

        Examples "示例"
        ```python
        from taxonomy import Taxonomy

        database = Taxonomy("databases/phytoplankton.db")
        names = database.retrieve_hierarchical_parents_by_child("genus", "角甲藻属", return_child=True, names_only=True)

        >>> names
        [('甲藻门', 'Dinophyta'), ('甲藻纲', 'Dinophyceae'), ('多甲藻目', 'Peridiniales'), ('角甲藻科', 'Ceratiaceae'), ('角甲藻属', 'Ceratium')]
        ```
        """
        parents = []
        if return_child:
            item = self.retrieve_data_by_chinese(child_taxon, child_chinese)
            parents.append(item)

        idx = self._taxa.index(child_taxon)
        while idx != 0:
            parent = self.retrieve_data_by_child(child_taxon, child_chinese)

            parents.insert(0, parent)
            idx -= 1
            child_taxon = self._taxa[idx]
            child_chinese = parent[0]

        if names_only:
            names = []
            for item in parents:
                names.append(item[:2])
            return names
        else:
            return parents

    def rollback(self) -> None:
        """数据库回滚。

        封装了SQL中的`rollback;`语句，用于想撤销改动时调用。
        """
        self._connect.rollback()

    def update_chinese(
        self,
        taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
        chinese: str,
        new_chinese: str,
    ) -> None:
        """更新生物的中文名。

        Args:
            taxon: 生物的类别。
            chinese: 生物的中文名。
            new_chinese: 生物的新中文名。
        """
        self._execute(commands.update_chinese(taxon, chinese, new_chinese))

    def update_scientific(
        self,
        taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
        chinese: str,
        new_scientific: str,
    ) -> None:
        """更新生物的学名。

        Args:
            taxon: 生物的类别。
            chinese: 生物的中文名。
            new_scientific: 生物的新学名。
        """
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

    def _fetchall(self) -> list[tuple]:
        return self._cursor.fetchall()

    def _fetchone(self) -> tuple:
        return self._cursor.fetchone()
