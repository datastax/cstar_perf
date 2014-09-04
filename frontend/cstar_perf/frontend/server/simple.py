from filters import val_ago, human_date
from flask import render_template, Flask
import datetime as dt

app = Flask(__name__)

def top_articles():
    articles = [
        {"title": "Google", "score": 150, "link": "http://google.com"},
        {"title": "Yahoo", "score": 75, "link": "http://yahoo.com"},
        {"title": "Bing", "score": 50, "link": "http://bing.com"}
    ]
    return articles

@app.route('/')
def index():
    articles = top_articles()
    return render_template("index.jinja2.html", rows=articles)

@app.template_filter()
def seconds_ago(val):
    return val_ago(val, unit="second")

@app.route('/experiment')
def experiment():
    return render_template('seconds.jinja2.html',
                           seconds=range(60))

app.add_template_filter(human_date)

@app.route('/datetest')
def datetest():
    now = dt.datetime.now()
    deltas = [
        dt.timedelta(seconds=5),
        dt.timedelta(seconds=60*60),
        dt.timedelta(days=5),
        dt.timedelta(days=60)
    ]
    dates = [(now - delta)
                for delta in deltas]
    return render_template('dates.jinja2.html',
                            dates=dates)

if __name__ == "__main__":
    app.run(debug=True)
