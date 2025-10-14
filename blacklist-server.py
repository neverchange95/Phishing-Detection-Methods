from flask import Flask, request

app = Flask(__name__)

@app.post("/url-list")
def url_list():
    data = request.get_json(force=True)
    print(data)
    return {"ok": True}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)