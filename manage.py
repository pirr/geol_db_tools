#-*- coding: utf-8 -*-


from flask_script import Manager
from flask_migrate import Migrate, MigrateCommand
from setup import app, db


migrate = Migrate(app, db)

manager = Manager(app)
manager.add_command('db', MigrateCommand)


class User(db.Model):
    """
    db table for user's data
    :param str email: email address of user
    :param str password: encrypted password for the user
    """
    __tablename__ = 'user'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(35), unique=True)
    password = db.Column(db.String(14))

    def __init__(self, email, password):
        self.email = email
        self.password = password

    def __repr__(self):
        return '<User %r>' % self.email


if __name__ == '__main__':
    manager.run()
