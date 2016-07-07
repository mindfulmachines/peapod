var inputGraph = document.querySelector("#inputGraph");
var render = dagreD3.render();
function tryDraw() {
    var xmlhttp = new XMLHttpRequest();
    var query = window.location.search.substring(1);
    var url;
    if (query == "active") {
        url = "graph?active";
    } else {
        url = "graph"
    }

    xmlhttp.onreadystatechange = function () {
        if (xmlhttp.readyState == 4 && xmlhttp.status == 200) {
            var myArr = JSON.parse(xmlhttp.responseText);
            tryDrawGraph(myArr);
        }
    };
    xmlhttp.open("GET", url, true);
    xmlhttp.send();
}
function tryDrawGraph(graph) {
    var g = new dagreD3.graphlib.Graph().setGraph({});

    for(i in graph.nodes) {
        n = graph.nodes[i];
        var classes = "";
        if(n.ephemeral) {classes = classes + " dashed"}
        if(n.exists) {classes = classes + " filled"}
        g.setNode(n.name, {class: classes});
    }

    for(i in graph.edges) {
        e = graph.edges[i];
        g.setEdge(e.nodeA, e.nodeB,{});
    }

    // Create the renderer
    var render = new dagreD3.render();
    // Set up an SVG group so that we can translate the final graph.
    var svg = d3.select("svg"), 
        inner = svg.append("g");

    // Set up zoom support
    var zoom = d3.behavior.zoom().on("zoom", function() {
        inner.attr("transform", "translate(" + d3.event.translate + ")" +
            "scale(" + d3.event.scale + ")");
    });
    svg.call(zoom);

    // Run the renderer. This is what draws the final graph.
    render(inner, g);
    // Center the graph
    //var xCenterOffset = (svg.attr("width") - g.graph().width) / 2;
    //inner.attr("transform", "translate(" + xCenterOffset + ", 20)");
    //svg.attr("height", g.graph().height + 40);
}