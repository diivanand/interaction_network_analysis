/*
 * @author Diivanand Ramalingam
 */

//Global Variables
    
    var graph;
    var links;
    var nodeProps;
    var w, h; // width and height of the SVG respectively
    var nodes;
    var linkDict;
    var numNodes;
    var force;
    var svg;
    var nodeColor;
    var nodeSize;
    var size_extent;
    var size_scale;
    var path_extent;
    var path_scale;
    var loading;
    var path;
    var circle;
    var text;
    var count = 1; //Used when trying to get the approciate connected component json file
                   //I start at 1 instead of 0 because the 1st CC takes a bit long to load
                   //So I start at the 2nd CC since it loads fast and allows for quick testing
                   //I will change this back to 0 when the app is complete

/*
 * @param arr a javascript array
 * @return an array with duplicate items removed
 */
function unique(arr) {
    var hash = {}, result = [];
    for (var i = 0, l = arr.length; i < l; ++i) {
        if (!hash.hasOwnProperty(arr[i])) {//it works with objects! in FF, at least
            hash[arr[i]] = true;
            result.push(arr[i]);
        }
    }
    return result;
}

/**
 * Updates the html header to display the current component number
 * @param count the connected component number to be displayed
 */
function displayComponentNum(count) {
    console.log("Entered displayComponentNum");
    $("#cc_head").text('Component No: ' + (count + 1));
}

/**
 * 
 * @param command a string that determines how count is to be altered before calling drawGraph()
 */
function draw(command) {
    console.log("draw called with: " + command);
    var folder = "inter_graph_CCs/";

    if (command === 'start') {
        count = 1;
        d3.json(folder + count + ".json", function(d) {
            drawGraph(d, count, command)
        });
    }

    if (command === 'prev') {
        if (count > 0) {
            count = count - 1;
            d3.json(folder + count + ".json", function(d) {
                drawGraph(d, count, command);
            });
        } else {
            console.log("Index out of Bounds Error");
        }
    }

    if (command === 'next') {
        if (count < 87) {
            count = count + 1;
            d3.json(folder + count + ".json", function(d) {
                drawGraph(d, count, command);
            });
        } else {
            console.log("Index out of Bounds Error");
        }
    }

    if (command === 'current') {
        d3.json(folder + count + ".json", function(d) {
            drawGraph(d, count, command);
        });
    }
}

/**
 * 
 * @param data json data which contains information on the nodes and links in the graph
 * @param count the number of the connected component being drawn
 * @param command a string that determined how objects will be redrawn
 */
function drawGraph(data, count, command){
    displayComponentNum(count);
    
    //Resetting for redrawing a new connected component
    if (command === 'prev') {
        $("#g" + (count + 1)).remove();
        resetAll();
    }
    if (command === 'next') {
        $("#g" + (count - 1)).remove();
        resetAll();
    }
        
    //predefined variables
    graph = data;
    links = graph.links;
    nodeProps = graph.nodes;
	w = 600;
	h = 480;
    nodes = {};
    linkDict={};
    numNodes = nodeProps.length;
    
   
    // Start temporary code 1 to be removed once pan and zoom is added to svg
    if (nodeProps.length > 20) {
        w = 2000;
        h = 2000;
    }

    if (nodeProps.length > 30) {
        w = 3000;
        h = 3000;
    }
    // End temporary code 1
    
    //Attach node properties to node
    nodeProps.forEach(function(node) {
        nodes[node.id] = node;
    });

    // Attach the nodes to the links
    links.forEach(function(link) {
        link.source = nodes[link.source];
        link.target = nodes[link.target];
        link.type = "directed";
        
        linkDict[link.source.id + "_" + link.target.id] = link;
    });
    
    // Define the undefined variables
    
    force = d3.layout.force() // initializes the d3 force layout
                       .nodes(d3.values(nodes))
                       .links(links)
                       .size([w,h])
                       .linkDistance(300)
                       .charge(-1000)
                       .on("tick",tick);
    
    svg = d3.select("#chart") // initialize the svg container that the graph will be drawn in
		    .on("mousewheel", blockScroll)
		    .on("DOMMouseScroll", blockScroll)
            .append("svg:svg")
              .attr("width",w)
              .attr("height",h)
 		      .attr("pointer-events", "all")
		      .call(d3.behavior.zoom()
		      .on("zoom", redraw))
              .attr("class", "svg_graph")
              .attr("id","g" + count)
    		.append("g");;

    path_extent = d3.extent(links, function(link){ 
        return parseFloat(link.value);
    }); // get the minimum and maximum edge weights
                       
   path_scale = d3.scale.linear()
                          .domain(path_extent)
                          .range([2.0,4.5]); //get a linear scaling function to determine edge thickness
                       
   loading = d3.select("#chart")
               .append("text")
                 .attr("id", "render_txt")
                 //.attr("x", w / 3.5)
                 //.attr("y", h / 8).attr("dy", ".35em")
                 .attr("text-anchor", "middle")
                 .text("Rendering. One moment please…"); //loading message displayed until graph is drawn
                
   path = svg.append("svg:g")
             .selectAll("path")
             .data(force.links())
             .enter()
             .append("svg:path")
               .attr("class", "link")
               .attr("id", function(link){
                   return link.source.id + "_" + link.target.id;
               })
               .attr("stroke-width", function(link) {
                   return "" + path_scale(parseFloat(link.value)) + "px";
               })
               .attr("marker-end", function(link) {
                   return "url(#" + link.type + ")";
               }); //initalizes each svg path element and appends data to it using D3
   
   circle = svg.append("svg:g")
               .selectAll("node")
               .data(force.nodes())
               .enter()
               .append("svg:circle")
                 .attr("class", "node")
                 .attr("id", function(node){
                     return node.id;
                 })
                 .attr("r", 6.0)
                 .attr("fill","#ccc");
                 
   text = svg.append("svg:g")
             .selectAll("g")
             .data(force.nodes())
             .enter()
             .append("svg:g"); //Text placed next to each node
             
   // Action and Drawing code starts here
   
   
   //give initial positions to all the nodes, diagonally to allow less iterations of layout algorithm
   force.nodes().forEach(function(node, i) {
        node.x = node.y = w / nodeSize * i;
   });
   
   
   
   
   $("#g" + count).hide();
   setTimeout(function() {
       $("#g" + count).show();
       force.start();
       for (var i = force.nodes().length * force.nodes().length + 5000; i > 0; --i)
           force.tick(); //Runs one iteration of the layout algorithm
       force.stop();
       loading.remove();
   }, 1000); // Let the force layout algorithm run so we have a nicely laid out static graph when done
   
   
   // Per-type markers, as they don't inherit styles.
    svg.append("svg:defs")
       .selectAll("marker")
       .data(["suit", "licensing", "resolved", "directed"])
       .enter()
       .append("svg:marker")
         .attr("id", String)
         .attr("viewBox", "0 -5 10 10")
         .attr("refX", 15)
         .attr("refY", -1.5)
         .attr("markerWidth", 6)
         .attr("markerHeight", 6)
         .attr("orient", "auto")
       .append("svg:path")
         .attr("d", "M0,-5L10,0L0,5");
           
    //If you want to be able to drag the circles, uncomment this
    //circle.call(force.drag);
    
    // A copy of the text with a thick white stroke for legibility.
    text.append("svg:text")
          .attr("x", 8)
          .attr("y", ".31em")
          .attr("class", "shadow")
          .text(function(node) {
              return node.Title;
          });

    text.append("svg:text")
          .attr("x", 8)
          .attr("y", ".31em")
          .text(function(node) {
              return node.Title;
    });
    
    
    //Event Driven Code
    
    d3.selectAll("circle.node").on("click", function() {
        console.log("displaying node props");
        console.log(this);
        $(".link.active").attr("class","link");
        $(".node.active").attr("class","node");
        resetInterProps();
        highlightNodeAndNeighbors(nodes[this.id]);
        displayNodeProps(nodes[this.id]);
        displayEdgePropsNodeClick(nodes[this.id]);
    });
    
    d3.selectAll("path.link").on("click", function() {
        $(".link.active").attr("class","link");
        $(".node.active").attr("class","node");
        resetNodeProps();
        resetInterProps();
        highlightEdge(this);
        displayEdgePropsEdgeClick(this);
    });
    
    d3.selectAll(".node_color_select").on("click", function(){
        $(".color_legend_element").remove();
        console.log($(this).text());
        var selection = $(this).text();
        var color = d3.scale.category20(); // function that maps objects to colors
        var colList = [];
                
        if(selection === 'Country'){
            d3.selectAll(".node")
                .attr('fill', function(node){
                    return color(node["Official Country"]);
                });
            
            var circleArray = $(".node");
            
            for(var i = 0;i < circleArray.length;i++){
                colList[i] = nodes[circleArray[i].id]["Official Country"];
            }
            
            var unq_col_list_sorted = unique(colList).sort();
            console.log(unq_col_list_sorted);
           
           
            unq_col_list_sorted.forEach(function(d,i){
               var idString = 'color_legend_element_' + d;
               var classString = 'color_legend_element';
               var styleString = String(color(String(d)));
               
               var wholeIDString = 'id="' + idString+'"';
               var wholeClassString = 'class="' + classString+'"';
               var wholeStyleString = 'style="color:' + styleString + '"';
               var propString = ' '+wholeIDString+' '+wholeClassString+' '+wholeStyleString;
               
               $("#color_legend_list").append('<li'+propString+'>'+String(d)+'</li>');
            });
            
            
        }else if(selection === 'City'){
            d3.selectAll(".node")
                .attr('fill', function(node){
                    return color(node["Official City"]);
                });
            
            var circleArray = $(".node");
            
            for(var i = 0;i < circleArray.length;i++){
                colList[i] = nodes[circleArray[i].id]["Official City"];
            }
            
            var unq_col_list_sorted = unique(colList).sort();
            
            unq_col_list_sorted.forEach(function(d,i){
               var idString = 'color_legend_element_' + d;
               var classString = 'color_legend_element';
               var styleString = String(color(String(d)));
               
               var wholeIDString = 'id="' + idString+'"';
               var wholeClassString = 'class="' + classString+'"';
               var wholeStyleString = 'style="color:' + styleString + '"';
               var propString = ' '+wholeIDString+' '+wholeClassString+' '+wholeStyleString;
               
               $("#color_legend_list").append('<li'+propString+'>'+String(d)+'</li>');
            });
                
                
        }else if(selection === 'Department'){
            d3.selectAll(".node")
                .attr('fill', function(node){
                    return color(node["Department"]);
                });
                
            var circleArray = $(".node");
            
            for(var i = 0;i < circleArray.length;i++){
                colList[i] = nodes[circleArray[i].id]["Department"];
            }
            
            var unq_col_list_sorted = unique(colList).sort();
            
            unq_col_list_sorted.forEach(function(d,i){
               var idString = 'color_legend_element_' + d;
               var classString = 'color_legend_element';
               var styleString = String(color(String(d)));
               
               var wholeIDString = 'id="' + idString+'"';
               var wholeClassString = 'class="' + classString+'"';
               var wholeStyleString = 'style="color:' + styleString + '"';
               var propString = ' '+wholeIDString+' '+wholeClassString+' '+wholeStyleString;
               
               $("#color_legend_list").append('<li'+propString+'>'+String(d)+'</li>');
            });
            
        }else{
            d3.selectAll(".node")
                .attr("fill","#ccc");
        }
        
        
    });
    
    d3.selectAll(".node_size_select").on("click", function(){
        console.log($(this).text());
        var selection = $(this).text();
        var size_extent;
        var size_scale;
        
        if(selection === 'InDegree'){
            size_extent = d3.extent(data.nodes, function(node) {
                   return parseFloat(node.inDegree);
            });
            size_scale = d3.scale.linear()
                                 .domain(size_extent)
                                 .range([6.0,20.0]);
            d3.selectAll(".node")
                .attr("r",function(node){
                    return size_scale(parseInt(node.inDegree));  
                });
        }else if(selection === 'OutDegree'){
            size_extent = d3.extent(data.nodes, function(node) {
                   return parseFloat(node.outDegree);
            });
            size_scale = d3.scale.linear()
                                 .domain(size_extent)
                                 .range([6.0,20.0]);
            d3.selectAll(".node")
                .attr("r",function(node){
                    return size_scale(parseInt(node.outDegree));  
                });
        }else if(selection === 'TotalDegree'){
            size_extent = d3.extent(data.nodes, function(node) {
                   return parseFloat(node.totalDegree);
            });
            size_scale = d3.scale.linear()
                                 .domain(size_extent)
                                 .range([6.0,20.0]);
            d3.selectAll(".node")
                .attr("r",function(node){
                    return size_scale(parseInt(node.totalDegree));  
                });
        }else{
            d3.selectAll(".node")
                .attr("r",6.0);
        }  
    });
    
    //Tick function, the layout algorithm 
    
    // Use elliptical arc path segments to doubly-encode directionality.
    function tick() {
        path.attr("d", function(d) {
            var dx = d.target.x - d.source.x;
            var dy = d.target.y - d.source.y;
            var dr = Math.sqrt(dx * dx + dy * dy);
            return "M" + d.source.x + "," + d.source.y + "A" + dr + "," + dr + " 0 0,1 " + d.target.x + "," + d.target.y;
        });

        circle.attr("transform", function(d) {
            return "translate(" + d.x + "," + d.y + ")";
        });

        text.attr("transform", function(d) {
            return "translate(" + d.x + "," + d.y + ")";
        });
    }
   
}

function highlightNodeAndNeighbors(node){
    console.log("Entered highlightNodeAndNeighbors:");
    console.log(node);
    var predecessors = node["predecessors"];
    var successors = node["successors"];
    $("#"+node.id).attr("class","node active main");
    
    for(var i=0;i < predecessors.length;i++){
        var predID = predecessors[i]
        var linkID = predID + "_" + node.id;
        $("#" + linkID).attr("class","link active");
        $("#" + predID).attr("class","node active");
    }
    for(var i=0;i < successors.length;i++){
        var succID = successors[i];
        var linkID = node.id + "_" + successors[i];
        $("#" + linkID).attr("class","link active");
        $("#" + succID).attr("class","node active");
    }
}

function highlightEdge(path_obj){
    console.log("Entered highlightEdge:");
    console.log(path_obj);
    $(path_obj).attr("class", "link active");
    var idArr = path_obj.id.split('_');
    var src = nodes[idArr[0]];
    var trg = nodes[idArr[1]];
    $("#"+src.id).attr("class", "node active");
    $("#"+trg.id).attr("class", "node active");
}

function displayNodeProps(node){
    console.log("Entered displayNodeProps: ");
    console.log(node);
    $("#country_property").text("Country: " + node["Official Country"]);
    $("#city_property").text("City: " + node["Official City"]);
    $("#department_property").text("Department: " + node["Department"]);
    $("#title_property").text("Title: " + node["Title"]);
    
}

function resetAll(){
    console.log("Entered resetAll()");
    
    resetNodeProps();
    resetInterProps();
    
    $(".color_legend_element").remove();
}

function displayEdgePropsNodeClick(node){
    console.log("Entered displayEdgePropsNodeClick: ");
    console.log(node);
    
    var predecessors = node["predecessors"];
    var successors = node["successors"];
    
    var countsDict = {};
    
    for(var i=0;i < predecessors.length;i++){
        var predID = predecessors[i];
        var linkID = predID + "_" + node.id;
        var link = linkDict[linkID];
        var nameDict = link["lcnt"];
        var nameArr = Object.keys(nameDict);
        
        
        nameArr.forEach(function(name){
           if(name in countsDict){
               countsDict[name] = countsDict[name] + nameDict[name];
           }else{
               countsDict[name] = nameDict[name];
           }
        });   
    }
    
    for(var i=0;i < successors.length;i++){
        var succID = successors[i];
        var linkID = node.id + "_" + successors[i];
        var link = linkDict[linkID];
        var nameDict = link["lcnt"];
        var nameArr = Object.keys(nameDict);
        
        
        nameArr.forEach(function(name){
           if(name in countsDict){
               countsDict[name] = countsDict[name] + nameDict[name];
           }else{
               countsDict[name] = nameDict[name];
           }
        });
    }
    
    var classString = 'inter_prop_list_element';
    var wholeClassString = 'class="' + classString+'"';
    
    for(var name in countsDict){
        var kvText = name + ": " + countsDict[name];
        
        $("#inter_prop_list").append('<li '+wholeClassString+'><a href="#">'+kvText+'</a></li>');
    }

}

function displayEdgePropsEdgeClick(path_obj){
    console.log("Entered displayEdgePropsEdgeClick: ");
    console.log(path_obj);
    var link = linkDict[path_obj.id];
    var arrSize = link.ctype.length;
    var responderID = link.source.id;
    var creatorID = link.target.id;
    var responderTitle = link.source.Title;
    var creatorTitle = link.target.Title;
    var weight = link.value;
    var creaDict = {"DOC":'document',"COM":'comment',"STAT":'status update',
                   "BLOGP":'blogpost',"MSG":'message'};
    var respDict = {"MSG":'replied via a message to ',"LIKE":"liked ","COM":'commented on '};
    var locDict = {"BLOG":'blog',"SGROUP":'social group',"COMMU":'community',
                  "CONT":'user container',"PROJECT":'project',"NOLOC":'unknown location'};
    
    var headString = "User: " + responderID  +  "' responded to User: " + creatorID + " " + 
                     toWords(parseInt(weight)) + " times:";
    var classString = 'inter_prop_list_element';
    var wholeClassString = 'class="' + classString+'"';
    
    
    $("#inter_prop_list").append('<li '+wholeClassString+'>'+headString+'</li>');
    
    for(var i=0;i < arrSize;i++){
        
        var ctype = creaDict[link.ctype[i]];
        var rtype = respDict[link.rtype[i]];
        var rtime = parseInt(link.rtime[i]);
        var ltype = locDict[link.ltype[i]];
        var lname = link.lname[i];
        
        var date = new Date(rtime);
        
        var dateString = date.format("dddd, mmmm d, yyyy HH:MM:ss");
        
        
        var interString = rtype + " " + ctype + " located in the " + ltype + " named '" + lname + "' on " + dateString;
         
        
        $("#inter_prop_list").append('<li '+wholeClassString+'><a href="#">'+interString+'</a></li>');
    }
    
}

function resetNodeProps(){
    $("#country_property").text("");
    $("#city_property").text("");
    $("#department_property").text("");
    $("#title_property").text("");
}

function resetInterProps(){
    $(".inter_prop_list_element").remove();
}

function resetHighlighting(){
    resetNodeProps();
    resetInterProps();
    $(".link.active").attr("class","link");
    $(".node.active").attr("class","node");
}

function redraw() {
  svg
    .attr("transform",
      "translate(" + d3.event.translate + ")"
      + " scale(" + d3.event.scale + ")");
console.log(1/d3.event.scale);
}

function blockScroll() { d3.event.preventDefault(); }