if (!window.Monitrix)
	window.Monitrix = {};

Monitrix.PieChart = function(width, height, radius, data, id) {
  var color = d3.scale.category20c();
  var vis = d3.select("#" + id)
    .append("svg:svg")
    .data([data])     
    .attr("width", width)
    .attr("height", height)
    .append("svg:g")             
    .attr("transform", "translate(" + width/2 + "," + height/2 + ")");
    
  var arc = d3.svg.arc().outerRadius(radius);
  var arcHover = d3.svg.arc().outerRadius(radius + 10);

  var pie = d3.layout.pie().value(function(d) { return d.value; });  

  var arcs = vis.selectAll("g.slice")
    .data(pie) 
    .enter()  
    .append("svg:g")
    .attr("class", "slice")
    .on("mouseover", function(d) {
      d3.select(this).select("path").transition()
      .duration(100)
      .attr("d", arcHover);
     })
     .on("mouseout", function(d) {
       d3.select(this).select("path").transition()
       .duration(100)
       .attr("d", arc);
     });

  arcs.append("svg:path")
    .attr("fill", function(d, i) { return color(i); } ) 
    .attr("d", arc)
    .append("svg:title").text(function(d, i) { return d.value + " views" ; });

  arcs.append("svg:text")
    .attr("transform", function(d) { 
      d.innerRadius = 0;
      d.outerRadius = radius;
      return "translate(" + arc.centroid(d) + ")";
    })
    .attr("text-anchor", "middle")          
    .text(function(d, i) { return data[i].label; });	
}