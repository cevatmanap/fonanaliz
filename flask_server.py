import fon_query
import json, yaml
import seaborn
import matplotlib.pyplot as plt
import io, base64
import os

from flask import Flask, jsonify, request
dir_path = os.path.dirname(os.path.abspath(__file__))
conf_path = os.path.join(dir_path, "config.yaml")
config = yaml.safe_load(open(conf_path, "r"))

print(config)



app = Flask(__name__)
@app.route('/get_prices', methods=['POST'])
def get_prices():
    conn = fon_query.connect_db()
    e = request.get_json()
    
    fon_codes = e["codes"]
    dates = e["dates"]
    normalize = e["normalize"]

    images = []

    print("foncodes: ", fon_codes)
    print("dates: ", dates)
    print("norm: ", normalize)

    for f in fon_codes:
        img = io.BytesIO()
        d = fon_query.get_prices_between_dates(conn, f, dates["start"], dates["end"], normalize=normalize)
        seaborn.lineplot(data=d, x="date", y="price", label=d["code"][0])
        plt.savefig(img,dpi=500)
        plt.close()
        img.seek(0)
        images.append(base64.b64encode(img.getvalue()).decode('utf8'))
        
    fon_query.close_db(conn)
    return jsonify({"images":images})

if __name__ == '__main__':  
     app.run(host=config["server_hostname"], port=config["server_port"], debug=True)

