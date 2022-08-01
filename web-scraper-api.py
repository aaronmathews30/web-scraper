from flask import Flask, request
from flask.json import jsonify
import webscraper
app = Flask(__name__)


@app.route('/scrape', methods=['POST'])
def scrape_news():
    content = request.json

    url = content.get('url')
    
    m_dict = {'message': webscraper.web_scraper(url)}
    return jsonify(m_dict)

 
if __name__ == '__main__':
    app.run(debug=True)
