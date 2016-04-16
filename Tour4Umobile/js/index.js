//进度条加载
$("head").ready(function() {
	$(".progress-bar").animate({'width': '30%'},50);
});
$("body").ready(function() {
	$(".progress-bar").animate({'width': '80%'},50);
});
$("script").ready(function() {
	$(".progress-bar").animate({'width': '90%'},50);
});
$(document).ready(function() {
	//进度条加载完即消失
	$(".loading").fadeOut();
	
	$(function(){
   		 $('body').fullpage({
   		 	slidesNavigation: true,
   		 	loopHorizontal: false,
   		 	controlArrowColor: 'transparent',
   		 	continuousVertical: true
   		 });
	});

}); 