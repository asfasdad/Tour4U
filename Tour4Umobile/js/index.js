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
	$(".fp-slidesNav").css("margin-left", "-100px");
    
	$(function(){
   		 $('body').fullpage({
   		 	slidesNavigation: true,
   		 	loopHorizontal: false,
   		 	controlArrowColor: 'transparent',
   		 	continuousVertical: true,
                                          anchors: ['firstPage', 'secondPage', 'thirdPage', 'fourthPage','fifthPage','sixthPage', 'lastPage'],
                                          afterSlideLoad: function(anchorLink, index, slideAnchor, slideIndex) {
                                                if (slideIndex===1) {
                                                    $('#int-slide').addClass("animation");
                                                }
                                                if (slideIndex===2) {
                                                    $('#fb-slide').addClass("animation");
                                                }
                                                if (slideIndex===3) {
                                                    $('#food-slide').addClass("animation");
                                                }
                                                if (slideIndex===4) {
                                                    $('#play-slide').addClass("animation");
                                                }
                                                if (slideIndex===5) {
                                                    $('#car-slide').addClass("animation");
                                                }
                                                if (slideIndex===6) {
                                                    $('#horse-slide').addClass("animation");
                                                }
                                                if (slideIndex===7) {
                                                    $('#inf-slide').addClass("animation");
                                                }

                                          },
                                          onSlideLeave: function( anchorLink, index, slideIndex, direction, nextSlideIndex){
                                                if (slideIndex===1) {
                                                    $('#int-slide').removeClass("animation");
                                                }
                                                if (slideIndex===2) {
                                                    $('#fb-slide').removeClass("animation");
                                                }
                                                if (slideIndex===3) {
                                                    $('#food-slide').removeClass("animation");
                                                }
                                                if (slideIndex===4) {
                                                    $('#play-slide').removeClass("animation");
                                                }
                                                if (slideIndex===5) {
                                                    $('#car-slide').removeClass("animation");
                                                }
                                                if (slideIndex===6) {
                                                    $('#horse-slide').removeClass("animation");
                                                }
                                                if (slideIndex===7) {
                                                    $('#inf-slide').removeClass("animation");
                                                }
                                          }

           		 });
              });


                // $('body').fullpage({
                //     anchors: ['firstPage', 'secondPage', 'thirdPage', 'fourthPage', 'fifthPage','sixthPage','lastPage'],
                //     afterLoad: function(anchorLink, index){
                //         var loadedSection = $(this);
                //         //using index
                //         if(index == 0){
                //             alert("Section 3 ended loading");
                //         }
                //         //using anchorLink
                //         if(anchorLink == 'secondSlide'){
                //             alert("Section 2 ended loading");
                //         }
                //     }
                // });
}); 

// 当前屏幕的动画
