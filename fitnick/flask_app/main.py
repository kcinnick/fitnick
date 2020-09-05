from flask import Flask, make_response, render_template, request

app = Flask(__name__)


@app.route("/", methods=['GET'])
def index():
    return render_template("index.html")


@app.route("/get_heart_rate_zone_today", methods=['GET', 'POST'])
def get_heart_rate_zone_today():
    if request.method == 'POST':
        print("Get heart rate zone method.")
        return render_template("index.html", value='Today\'s heart rate data was placed in the database!')
    else:
        return render_template("index.html")


@app.route('/<page_name>')
def other_page(page_name):
    response = make_response(f'The page named {page_name} does not exist.', 404)
    return response


if __name__ == '__main__':
    app.run(debug=True)
