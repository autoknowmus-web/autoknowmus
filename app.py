from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def index():
    # We pass some mock database stats to make the site look alive
    stats = {
        "total_searches": "1,24,500+",
        "real_transactions": "8,432"
    }
    return render_template('index.html', stats=stats)

if __name__ == '__main__':
    app.run(debug=True, port=5000)