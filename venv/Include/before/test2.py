from flask import Flask, jsonify
import multiprocessing
import dataset
import time
import random

app = Flask(__name__)
DATABASE_URL = 'sqlite:///dev.db'

def add_person(name):
    """ Add a person to the db. """
    person = {'name': name, 'age': -1, 'status': 'processing'}
    db = dataset.connect(DATABASE_URL)
    db['people'].insert(person)
    return True

def update_person(name):
    """ Update a person in db with a fake long running process
    that guesses a random age. """
    time.sleep(10)  # simulate a long running process
    age = random.randint(1, 120)
    person = {'name': name, 'age': age, 'status': 'finished'}
    db = dataset.connect(DATABASE_URL)
    db['people'].update(person, ['name'])
    return True

def get_person(name):
    """ Retrieve a person from the db. """
    db = dataset.connect(DATABASE_URL)
    person = db['people'].find_one(name=name)
    return person

@app.route('/<name>')
def index(name):
    """ Get person. If name not found, add_person to db and start update_person. """
    if not get_person(name):
        add_person(name)
        thread = multiprocessing.Process(target=update_person, args=(name,))
        thread.start()
    person = get_person(name)
    return jsonify(person)

if __name__ == '__main__':
    app.run(debug=True)