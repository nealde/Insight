import numpy as np
import kafka
import time

def hit_n_times(prod, n):
    topic = "cloud"
    # msg = {"text": text, "tags": tags, "index": ind}
    msg_list = []
    for i in range(nn):
        k = np.random.randint(0,5)
        ind = "".join(
            random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
            for _ in range(8)
        )
        msg = {"text": text_list[k], "tags": tags[k], "index": ind}
        msg_list.append(msg)
    prod.send_messages(topic, [json.dumps(msg).encode("utf-8") for msg in msg_list])


def hit(n, pause=None):
    # connect to kafka cluster
    cluster = kafka.KafkaClient("10.0.0.11:9092")
    prod = kafka.SimpleProducer(cluster, async=False)

    # aim to hit it based on an input
    for i in range(10):
        hit_n_times(prod, 100)
        if pause is not None:
        # optional: pace yourself
            time.sleep(.001)
    return







text_list = ["""
I use VScode to edit python,but the compiler report some problems

c = np.array([0.05,0.27,0.19,0.185,0.185])
A = np.diag([0,0.025,0.015,0.055,0.026])
b = a*np.ones((5,1))
Aeq = np.array([[1,1.01,1.02,1.045,1.065]])
beq = np.array([1])
res = optimize.linprog(-c,A,b,Aeq,beq)  //there is a problem[![enter image description here][1]][1]
""","""

In one of the components in my client/components folder, I am importing three images from the public/images folder. At some point, webpack created a file for each of the images with hashed names like the following: 0e8f1e62f0fe5b5e6d78b2d9f4116311.png. If I delete those files, they do not get recreated and I would like for webpack to just use the images that are provided in the images folder.

Now, I am trying to deploy the application on a proxy server and the hash files are being successfully downloaded on page load but the images are not displaying. I have a hunch that fixing the original webpack issue will fix the issue with the proxy server but I'm not sure about that either.

root
├── client
│   └── components
├── database
├── public
│   ├── images
│   ├── app.js
│   └── index.html
└── server
    └── server.js

const path = require('path');

module.exports = {
  entry: path.resolve(__dirname, './client/index.js'),
  module: {
    loaders: [
      {
        test: /\.jsx?$/,
        exclude: /node_modules/,
        loader: 'babel-loader',
        query: {
          presets: ['react', 'es2015', 'env']
        },
      },
      {
        test: /\.png$/,
        use: 'file-loader'
      }
    ],
  },
  output: {
    path: path.join(__dirname, '/public'),
    filename: 'app.js',
  }
};

The above is my file structure. I've tried to play around with my current config but I struggle with setting up webpack. Any help with these issues would be appreciated.
""", """enter image description hereI am supposed to create a python function that reads csv files. I have most of it written, there are just a few problems that I am having. First, my mean, min, and max are all giving me the same numbers, and I don't know what's wrong. Lastly, I can't figure out how to work the standard deviation. I would really appreciate the help.""",
"""

I'm looking to iframe in a web form that has variable height depending on which page of the form the user is on, so that it shows properly for all pages and on mobile.

By setting the width it seems to display appropriately, however setting the height as shown in the html below seems to cut off all but the top of the form.

Because of the nature of how this form will be embedded (into a weebly site using the custom html option), it is not feasible to use css or javascript to accomplish this.
""","""

I'm setting up exception handling logic for a multipart project with common error page (that is hosted in other part of the project). When I tried to redirect to external URL on exception, tomcat 8.5.39 is showing default error instead. Funny thing is, this seems to work just fine in tomcat 8.5.38

I've tried many different exception handling techniques, but they all seem not to work for external redirects.

So currently, i have something like this in my web.xml file:

...
    <error-page>
        <error-code>404</error-code>
        <location>/error/error404</location>
    </error-page>
...

and for my Spring controller,

@Controller
@RequestMapping(value = "/error")
public class ErrorHandler{
...
    @GetMapping(value = "error404")
    public String error404(){
        return "redirect:http://{myproject}/{404errorPage}";
    }
...
}

I'm expecting this code to redirect the user to http://{myproject}/{404errorPage} when 404 error occurs, which works just fine in tomcat 8.5.38. But on 8.5.39, they seem to have changed error handling logic, and it will display default error page(browser default 404 page).

Any input or idea would be tremendously helpful.
""","""

I am trying to read a byte [] (varbinary) value from a column in sql server table. when I fetch the byte [] value from the column using a API , it return a varchar, instead of the value stored in the sql table column.

How to fetch the exact data using the API , I am trying to decrypt , using the binary value stored in the column.

I have updated the type to byte in the API code to return the Byte [] value but it returns a varchar,

public async Task<IActionResult> GetBinaryValue([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var Binary Value = await _context.GetBinaryValue.FindAsync(id);

            if (GetBinaryValue== null)
            {
                return NotFound();
            }

            return Ok(GetBinaryValue);
        }

 public partial class GetBinaryValue
    {
        public int Uniquekey{ get; set; }
        public string Value{ get; set; }
        public DateTime SysStart { get; set; }
        public DateTime SysEnd { get; set; }
        public byte [] BinarValue{ get; set; }
    }
}

Value in DB : 0x9D8E1A40CA08A569417699547667XXXX
Fetching the value via API : nY4aQMoIpWlBdplUdmeQXXXX

Expected result: 0x9D8E1A40CA08A569417699547667XXXX

""","""Variable Length Record Load the pipe-delimited file P. It is organized with 3 fields on each line: firstname|lastname|birthday. Search for the firstname F and lastname L, replacing the birthday with B. Write the file back out in the same pipe-delimited format.""",
"""

I want to store a Fetch API JSON as a JavaScript object, so I can use it elsewhere. The console.log test works, but I can't access the data.

The Following Works: It shows console entries with three to-do items:

 fetch('http://localhost:3000/api/todos')
    .then(data => data.json())
    .then(success => console.log(success));

The Following Does Not Work:

fetch('http://localhost:3000/api/todos')
.then(data => data.json())
.then(success => JSON.parse(success));

If I try to access success, it does not contain any data.

Have tried console.log, which works.

Have also tried the following, which works:

fetch('http://localhost:3000/api/todos')
    .then(res => res.json())
    .then(data => {
        let output = '';
        data.forEach(function (todo) {
        output += `
            <ul>
                <li>ID: ${todo.id}</li>
                <li>Title: ${todo.title}</li>
                <li>IsDone: ${todo.isdone}</li>
            </ul>
            `;
        });
        document.getElementById('ToDoList').innerHTML = output;
        return output;
    })
    .catch(err => console.log('Something went wrong: ', err));

However, I can't manually update inner HTML; I need the object to do other UX.
"""
]
tags_list = ["python,vscode-settings","javascript,webpack","python-3.x","html","spring, tomcat","sql-server,asp.net-core-2.0","android","javascript, json, api, fetch"]

if __name__ = "__main__":
    main()
