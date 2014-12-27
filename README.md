EdgeModel
=========

NoSQL/ORM/DAO bindings for python and sqlite.

This is a very small and simple library to work with Python and Sqlite.
It is almost a NoSQL, but you can model (or map) your database the way you like.

- sqlite version: sqlite3
- python version: python2.7


TODO
====

- joins
- benchmarks


Usage
======

```python
class Bank(EdgeModel):

    def __define_model__(self):
        self.table_name = 'bank'
        self.id_bank = IntegerField(PrimaryKey, AutoIncrement, FieldName='id_type_pessoa')
        self.name = IntegerField(NotNull, Unique, FieldName='name')


class Card(EdgeModel):

    def __define_model__(self):
        self.table_name = 'card'
        self.id_card = IntegerField(PrimaryKey, AutoIncrement, FieldName='id_card')
        self.description = TextField(Unique, FieldName='description')
        self.id_bank = IntegerField(NotNull, ForeignModel=Bank, FieldName='id_bank')

db = Database()
db.open('databasepath.db', True)
db.create_tables([Bank, Card])


b1 = Bank()
b1.name.set_value('Bank 1')
b1.save()

mycard = Card()
mycard.description.set_value('My card bank 1')
mycard.id_bank.set_value(itau)
mycard.save()
```
