EdgeModel
=========

Very Simple NoSQL/ORM bindings for python2 and sqlite3

Example:

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
db.create_tables([TipoPessoa, Pessoa])


itau = Bank()
itau.name.set_value('Itau')
itau.save()

mycard = Card()
mycard.description.set_value('my itau card')
mycard.id_bank.set_value(itau)
mycard.save()