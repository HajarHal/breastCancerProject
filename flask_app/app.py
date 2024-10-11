from flask import Flask, render_template, send_from_directory
import os

app = Flask(__name__, static_folder='assets', static_url_path='/assets')

def get_categories_and_graphs(cancer_type, category_type):
    folder_path = os.path.join('assets', 'graph', f'{category_type}_{cancer_type}')
    graphs = []
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            if filename.endswith('.html'):
                graph_name = filename.replace(f'{category_type}_{cancer_type}_', '').replace('_', ' ').replace('.html', '')
                graphs.append((graph_name, filename))  
    return graphs

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/<cancer_type>')
def cancer_type(cancer_type):
    return render_template('cancer_type.html', cancer_type=cancer_type)

@app.route('/<cancer_type>/<category_type>')
def category_type(cancer_type, category_type):
    graphs = get_categories_and_graphs(cancer_type, category_type)
    return render_template('category.html', cancer_type=cancer_type, category_type=category_type, graphs=graphs)

@app.route('/<cancer_type>/<category_type>/<graph>')
def graph(cancer_type, category_type, graph):
    graph_path = f'assets/graph/{category_type}_{cancer_type}/{graph}'
    return render_template('graph.html', graph_path=graph_path)



@app.route('/assets/graph/<path:filename>')
def serve_graph(filename):
    return send_from_directory('assets/graph', filename)

if __name__ == '__main__':
    app.run(debug=True)
