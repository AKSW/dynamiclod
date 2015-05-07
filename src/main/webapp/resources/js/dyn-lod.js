var w = window.innerWidth
|| document.documentElement.clientWidth
|| document.body.clientWidth;

var h = window.innerHeight
|| document.documentElement.clientHeight
|| document.body.clientHeight;

var width = w - 140;
var height = h - 160;

var color = d3.scale.category20();
  
var force = d3.layout.force()
//.charge(-120)
.gravity(.15)
.charge(function(d, i) { return i ? 0 : -5000; })
.linkDistance(150)
.size([width, height]);  
                  
                  
                  
var nodeMap = {};    
makeGraph("");


function makeGraph(param) { 
//	d3.json("../dataid/CreateD3JSONFormat?dataset="+param, function(error, circleData) {
	d3.json("../dataid/CreateD3JSONFormat?getAllDistributions", function(error, circleData) {
//		d3.json("http://vmdbpedia.informatik.uni-leipzig.de:8080/dataid/CreateD3JSONFormat?dataset="+param, function(error, circleData) {
		
		
		if(circleData.nodes.length==0)
			return;
	
	
d3.select("svg")
    .remove();

console.log(circleData);

//Create the SVG Viewport
var svgContainer = d3.select("body").append("svg")
                                     .attr("width",width)
                                     .attr("height",height)    .attr("pointer-events", "all")
                                     .append('svg:g')
                                     .call(d3.behavior.zoom().on("zoom", redraw))
                                   .append('svg:g');

svgContainer.append('svg:rect')
                                     .attr('width', w)
                                     .attr('height', h)
                                     .attr('fill', 'white');

                                 function redraw() {
                                   console.log("here", d3.event.translate, d3.event.scale);
                                   svgContainer.attr("transform",
                                       "translate(" + d3.event.translate + ")"
                                       + " scale(" + d3.event.scale + ")");
                                 }


circleData.nodes.forEach(function(x) { nodeMap[x.name] = x; });
circleData.links = circleData.links.map(function(x) {
  return {
    source: nodeMap[x.source],
    target: nodeMap[x.target],
    value: x.value
  };
});



//build the arrow.
svgContainer.append("svg:defs").selectAll("marker")
    .data(["end"])
  .enter().append("svg:marker")
    .attr("id", String)
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", 15)
    .attr("refY", -1.5)
    .attr("markerWidth", 6)
    .attr("markerHeight", 6)
    .attr("orient", "auto")
  .append("svg:path")
    .attr("d", "M0,-5L10,0L0,5");                                     
                                     
force
	.nodes(circleData.nodes)
	.links(circleData.links)
	.start();

var path = svgContainer.append("svg:g").selectAll("path")
.data(circleData.links)
.enter().append("svg:path")
.attr("class", "link")
.attr("marker-end", "url(#end)");


var circles = svgContainer.selectAll("circle")
                           .data(circleData.nodes)
                           .enter()
                           .append("circle");;
                           
var circleAttr = circles
    .attr("r", function (d) { return d.radius; })
    .attr("class", "node")
//    .on("click", function(d) { window.open(d["url"],"_blank"); })
    .on("click", function(d) { makeGraph(d.name.replace("#", "@@@@@@"));})
    .attr("cursor", "pointer")
    .style("fill", function (d) { return d.color; })
    .call(force.drag);



    
var text = svgContainer.selectAll("text")
	.data(circleData.nodes)
	.enter()
	.append("text")
	.attr("x", 0)
    .attr("dy", ".35em")
    .attr("text-anchor", "middle")
    .style("font-size","8px")
	.html(function (d) { return d.text; });
	                           

force.on("tick", function() {
	
	
	var q = d3.geom.quadtree(circleData.nodes),
    i = 0,
    n = circleData.nodes.length;

while (++i < n) q.visit(collide(circleData.nodes[i]));

	
	
    path.attr('d', function(d) {
    var deltaX = d.target.x - d.source.x,
        deltaY = d.target.y - d.source.y,
        dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY),
        normX = deltaX / dist,
        normY = deltaY / dist,
        sourcePadding = d.left ? 17 : 12,
        targetPadding = d.right ? 17 : 12,
        sourceX = d.source.x + (sourcePadding * normX),
        sourceY = d.source.y + (sourcePadding * normY),
        targetX = d.target.x - (targetPadding * normX),
        targetY = d.target.y - (targetPadding * normY);
    return 'M' + sourceX + ',' + sourceY + 'L' + targetX + ',' + targetY;
  });


      circles.attr('transform', function(d) {
    	    return 'translate(' + d.x + ',' + d.y + ')';
    	  });
  
  text
  .attr("x", function(d) { return d.x; })
  .attr("y", function(d) { return d.y; });
  
	})
	
	
	function collide(node) {
  var r = node.radius + 16,
      nx1 = node.x - r,
      nx2 = node.x + r,
      ny1 = node.y - r,
      ny2 = node.y + r;
  return function(quad, x1, y1, x2, y2) {
    if (quad.point && (quad.point !== node)) {
      var x = node.x - quad.point.x,
          y = node.y - quad.point.y,
          l = Math.sqrt(x * x + y * y),
          r = node.radius+10 + quad.point.radius+10;
      if (l < r) {
        l = (l - r) / l * .5;
        node.x -= x *= l;
        node.y -= y *= l;
        quad.point.x += x;
        quad.point.y += y;
      }
    }
    return x1 > nx2 || x2 < nx1 || y1 > ny2 || y2 < ny1;
  };
}



//create the zoom listener
var zoomListener = d3.behavior.zoom()
  .scaleExtent([0.1, 3])
  .on("zoom", zoomHandler);

// function for handling zoom event
function zoomHandler() {
  vis.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
}

// create the svg
rootSvg = d3.select("#tree-body").append("svg:svg");
/*
  creating your svg image here
*/

// apply the zoom behavior to the svg image
zoomListener(rootSvg);


});
}