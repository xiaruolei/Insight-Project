from flask import Flask, request, render_template
import pymysql

db = pymysql.connect("ec2-54-200-1-210.us-west-2.compute.amazonaws.com", "rosie", "root", "insight", use_unicode=True)

app = Flask(__name__)
#api = Api(app)

@app.route('/', methods=['GET', 'POST'])
def connectDB():
    if request.method =='POST':
        userId = request.form['userId']
        cursor = db.cursor()
        
        # recommend results
        result_statement = "SELECT title FROM result JOIN mapping WHERE result.movieId = mapping.movieId AND userId = %s"
        cursor.execute(result_statement,(userId, ))
        results = cursor.fetchall()
        print(results)

        # Watch History
        history_statement = "SELECT title, rating FROM history JOIN mapping WHERE history.movieId = mapping.movieId AND userId = %s ORDER BY rating DESC LIMIT 10"
        cursor.execute(history_statement,(userId, ))
        history = cursor.fetchall()
        print(history)

        return render_template('dashboard.html', results=results, history=history, userId=userId)
    return render_template('index.html')



if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port =5000)
