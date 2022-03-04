from flask import Flask, request, render_template
import hsfs
import pymysql
import requests
import json

app = Flask(__name__)
api_key = (
    "V0LOYgEK0NBlLqHH.DYZ3mhKCoGoaGniT56dg9D81d4IpFFLQzLAp6lLvze93wUe5Y6tg5d8rRtm9LLoT"
)
instance = "a644f670-97cd-11ec-b462-bf31af5db661.cloud.hopsworks.ai"
project = "book_recommendation"

connection = None
books_rec_fg = None
titles_fg = None


@app.route("/")
def hello():
    return render_template("index.html")


@app.route("/value", methods=["POST"])
def value():
    vector = get_vector(request.form)
    return render_template(
        "index.html",
        book=vector[0][0],
        v1=vector[1][0],
        v1_img=vector[1][1],
        v2=vector[2][0],
        v2_img=vector[2][1],
        v3=vector[3][0],
        v3_img=vector[3][1],
        v4=vector[4][0],
        v4_img=vector[4][1],
    )


def get_vector(request):
    data = (
        books_rec_fg.select_all()
        .filter(books_rec_fg.isbn == request["isbn"])
        .read(online=True, read_options={"external": True})
    )

    return [get_title_picture(isbn) for isbn in data.to_numpy()[0]]


def get_title_picture(isbn):
    data = (
        titles_fg.select(["book_title", "image_url_l"])
        .filter(titles_fg.isbn == isbn)
        .read(online=True, read_options={"external": True})
    )

    return data.to_numpy()[0]


if __name__ == "__main__":
    conn = hsfs.connection(
        instance, 443, project, api_key_value=api_key, hostname_verification=True
    )

    fs = conn.get_feature_store()
    books_rec_fg = fs.get_feature_group("books_rec", version=1)
    titles_fg = fs.get_feature_group("books_titles", version=1)

    app.run()
