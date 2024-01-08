from typing import Literal


def create_data(
    taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
    values: list[str],
) -> str:
    return f"""
insert into "{taxon}"
values (*{values});
"""


def create_table_class_to_species(
    taxon: Literal["class", "order", "family", "genus", "species"],
    parent_taxon: Literal["phylum", "class", "order", "family", "genus"],
) -> str:
    return f"""
create table "{taxon}"
(
    chinese text NOT NULL,
    scientific text NOT NULL,
    description text NOT NULL,
    parent text NOT NULL,
    primary key (chinese),
    foreign key (parent) references "{parent_taxon}" (chinese)
);
"""


def create_trigger_update_chinese(
    taxon: Literal["phylum", "class", "order", "family", "genus"],
    child_taxon: Literal["class", "order", "family", "genus", "species"],
) -> str:
    return f"""
create trigger update_{taxon}_chinese
    after update
    on "{taxon}"
begin
    update "{child_taxon}"
    set parent = new.chinese
    where parent = old.chinese;
end;
"""


def create_trigger_delete_taxon(
    taxon: Literal["phylum", "class", "order", "family", "genus"],
    child_taxon: Literal["class", "order", "family", "genus", "species"],
) -> str:
    return f"""
create trigger delete_{taxon}
    after delete
    on "{taxon}"
begin
    delete from "{child_taxon}" where parent = old.chinese;
end;
"""


def delete_data(
    taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
    chinese: str,
) -> str:
    return f"""
delete
from "{taxon}"
where chinese = '{chinese}';
"""


def retrieve_data_by_child(
    taxon: Literal["phylum", "class", "order", "family", "genus"],
    child_taxon: Literal["class", "order", "family", "genus", "species"],
    child_chinese: str,
) -> str:
    return f"""
select *
from "{taxon}"
where chinese = (select parent
                 from "{child_taxon}"
                 where chinese = '{child_chinese}')
"""


def retrieve_data_by_chinese(
    taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
    chinese: str,
) -> str:
    return f"""
select *
from "{taxon}"
where chinese = '{chinese}';
"""


def retrieve_data_by_parent(
    parent_taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
    parent_chinese: str,
) -> str:
    return f"""
select *
from "{parent_taxon}"
where parent = '{parent_chinese}';
"""


def retrieve_data_by_taxon(
    taxon: Literal["phylum", "class", "order", "family", "genus", "species"]
) -> str:
    return f"""
select *
from "{taxon}";
"""


def update_chinese(
    taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
    chinese: str,
    new_chinese: str,
) -> str:
    return f"""
update "{taxon}"
set chinese = '{new_chinese}'
where chinese = '{chinese}';
"""


def update_scientific(
    taxon: Literal["phylum", "class", "order", "family", "genus", "species"],
    chinese: str,
    new_scientific: str,
) -> str:
    return f"""
update "{taxon}"
set scientific = '{new_scientific}'
where chinese = '{chinese}';
"""


COMMIT = "commit;"

CREATE_TABLE_PHYLUM = """
create table phylum
(
    chinese text NOT NULL,
    scientific text NOT NULL,
    description text NOT NULL,
    primary key (chinese)
);
"""

INITIALIZE = "pragma foreign_keys = 1;"

ROLLBACK = "rollback;"
