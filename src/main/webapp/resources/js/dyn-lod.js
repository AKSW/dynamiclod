

var w = window.innerWidth || document.documentElement.clientWidth
		|| document.body.clientWidth;

var h = window.innerHeight || document.documentElement.clientHeight
		|| document.body.clientHeight;

// var width = w - 240;
// var height = h - 240;
var width = 740;
var height = 650;

var level = 4;


// console.log(getUrlParameter("dataset"));
var showAll = false;

var requestLink;

var baseRequestLink = "/dataid/CreateD3JSONFormat2?";

//if (typeof getUrlParameter("getAllDistributions") != 'undefined') {
//	requestLink = baseRequestLink + "getAllDistributions=" + "&";
//}
if (typeof getUrlParameter("dataset") != 'undefined') {
	requestLink = getUrlParameter("dataset") + "&";
	if(getUrlParameter("dataset") == "")
		showAll = true;
	makeGraph(requestLink)
}
else makeGraph("");


function makeGraph(param) {
	if (param != "") {
		param = param.replace(new RegExp("#", "g"), '%23');
		param = param.replace(new RegExp("_anchor", "g"), '');
		requestLink = "/dataid/CreateD3JSONFormat2?dataset=" + param;		
	}
	if(showAll){
		requestLink = baseRequestLink + "getAllDistributions=" + "&";
	}
	console.log(param);
		

	var color = d3.scale.category20();

	var force = d3.layout.force().linkStrength(0.00001).friction(0.4)
			.linkDistance(50).charge(5).gravity(0.0000001).theta(1.9)
			.alpha(1.9).size([ width, height ]).start();

	
	var nodeMap = {};

	$.post(requestLink,
					function(circleData) {
						$("#loading_gif").hide();
						$("#filter").prop('disabled', false);
						
						d3.select("svg").remove();
						if (circleData.nodes.length == 0)
							return;

						console.log(circleData);

						// Create the SVG Viewport
						var svgContainer = d3.select("#diagram").append("svg")
								.attr("width", width).attr("height", height)
								.attr("pointer-events", "all")
								// .append('svg:g')
								.call(d3.behavior.zoom().on("zoom", redraw))
								.append('svg:g');

						svgContainer.append('svg:rect').attr('width', w).attr(
								'height', h).attr('fill', 'white');

						function redraw() {
							console.log("here", d3.event.translate,
									d3.event.scale);
							svgContainer.attr("transform", "translate("
									+ d3.event.translate + ")" + " scale("
									+ d3.event.scale + ")");
						}

						circleData.nodes.forEach(function(x) {
							nodeMap[x.name] = x;
						});
						circleData.links = circleData.links.map(function(x) {
							return {
								source : nodeMap[x.source],
								target : nodeMap[x.target],
								value : x.value
							};
						});

						// build the arrow.
						svgContainer
						.append("svg:defs").selectAll("marker")
								.data([ "end" ]).enter().
								append("svg:marker")
								.attr("id", String)
								.attr("viewBox", "0 -5 10 10")
								.attr("refX", 27)
								.attr("refY", -1)
								.attr("markerWidth", 6)
								.attr("markerHeight", 6)
								.attr("orient", "auto")
								.append("svg:path")
								.attr("d", "M0,-5L10,0L0,5");
						

						force.nodes(circleData.nodes).links(circleData.links)
								.start();
						
						// tooltip for links
						var tooltip = d3.select("body")
					    .append("div")
					    .attr('class', 'tooltip2')
					    .style("position", "absolute")
					    .style("z-index", "10")
					    .style("visibility", "hidden");

						// links
						var path = svgContainer.append("svg:g").selectAll(
								"path").data(circleData.links).enter().append(
								"svg:path").attr("class", "link")
								.style("stroke", function(d){ 
									if(d.value>0) return "#699"; 
									else return "#666";
								})
								.on("mouseover", function(d){
									d3.select(this).style("stroke", "red");
									if(d.value>0)
										tooltip.text("Links: "+d.value); 
									else
										tooltip.text("a dcat:subset ."); 										
									return tooltip.style("visibility", "visible"); 
								})
								.on("mousemove", function(){
									return tooltip.style("top", (d3.event.pageY-10)+"px").style("left",(d3.event.pageX+10)+"px");})
								.on("mouseout", function(){
									d3.select(this).style("stroke", function(d){ 
										if(d.value>0) return "#699"; 
										else return "#666";
									});
									return tooltip.style("visibility", "hidden");
								})								
								.attr(
								"marker-end", "url(#end)");
						
						

						// bubbles
						var circles = svgContainer.selectAll("circle").data(
								circleData.nodes).enter().append("circle");

						// attributes for bubbles
						var circleAttr = circles.attr("r", function(d) {
							return d.radius;
						})
							.attr("class", "node")
							.attr("cursor", "pointer").style("fill", function(d) {
							return d.color;
						})
							.call(force.drag);

						// var text = svgContainer.selectAll("text")
						// .data(circleData.nodes)
						// .enter()
						// .append("text")
						// //.attr("x", 0)
						// //.attr("dy", ".35em")
						// .attr("text-anchor", "middle")
						// .style("font-size","8px")
						// .text(function (d) { return d.text; })
						// .call(wrap, 70);

						var text = svgContainer
								.selectAll("foreignObject")
								.data(circleData.nodes)
								.enter()
								.append("foreignObject")
								.attr('width', function(d) {
									return d.radius + 15;
								})
								.attr('height', function(d) {
									return d.radius + 14;
								})
								.html(function(d) {
									return "<div style=\"font-size: 8px; text-align:center\">" + d.text + "</div>";
								});

						// config force
						force.on("tick",function() {
							var q = d3.geom.quadtree(circleData.nodes), i = 0, n = circleData.nodes.length;
							while (++i < n)
								q.visit(collide(circleData.nodes[i]));

							path.attr('d',function(d) {
								var deltaX = d.target.x
								- d.source.x, deltaY = d.target.y
								- d.source.y, dist = Math
								.sqrt(deltaX
										* deltaX
										+ deltaY
										* deltaY), normX = deltaX
										/ dist, normY = deltaY
										/ dist, sourcePadding = d.left ? 17
												: 12, targetPadding = d.right ? 17
														: 12, sourceX = d.source.x
														+ (sourcePadding * normX), sourceY = d.source.y
														+ (sourcePadding * normY), targetX = d.target.x
														- (targetPadding * normX), targetY = d.target.y
														- (targetPadding * normY);
								
								return 'M' + sourceX + ',' + sourceY + 'L' + targetX + ',' + targetY;
							});

							circles.attr('transform', function(d) {
								return 'translate(' + d.x + ','
								+ d.y + ')';
							});
							
							text.attr("x", function(d) {
								return d.x - d.radius / 2 - 6;
							}).attr("y", function(d) {
								return d.y - d.radius / 2 - 7;
							});
							
						})

						function collide(node) {
							var r = node.radius + 16, nx1 = node.x - r, nx2 = node.x
									+ r, ny1 = node.y - r, ny2 = node.y + r;
							return function(quad, x1, y1, x2, y2) {
								if (quad.point && (quad.point !== node)) {
									var x = node.x - quad.point.x, y = node.y
											- quad.point.y, l = Math.sqrt(x * x
											+ y * y), r = node.radius + 10
											+ quad.point.radius + 10;
									if (l < r) {
										l = (l - r) / l * .5;
										node.x -= x *= l;
										node.y -= y *= l;
										quad.point.x += x;
										quad.point.y += y;
									}
								}
								return x1 > nx2 || x2 < nx1 || y1 > ny2
										|| y2 < ny1;
							};
						}
						
						
						
						
						
						
						

						// create the zoom listener
//						var zoomListener = d3.behavior.zoom().scaleExtent(
//								[ 0.1, 3 ]).on("zoom", zoomHandler);

						// function for handling zoom event
//						function zoomHandler() {
//							vis.attr("transform", "translate("
//									+ d3.event.translate + ")scale("
//									+ d3.event.scale + ")");
//						}

						// create the svg
//						rootSvg = d3.select("#tree-body").append("svg:svg");
						/*
						 * creating your svg image here
						 */

						// apply the zoom behavior to the svg image
//						zoomListener(rootSvg);
					}, 'json');
}

function getUrlParameter(sParam) {
	var sPageURL = window.location.search.substring(1);
	var sURLVariables = sPageURL.split('&');
	for (var i = 0; i < sURLVariables.length; i++) {
		var sParameterName = sURLVariables[i].split('=');
		if (sParameterName[0] == sParam) {
			return sParameterName[1];
		}
	}
}
