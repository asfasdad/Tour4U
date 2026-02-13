//进度条加载
$("head").ready(function() {
	$(".progress-bar").animate({'width': '30%'},50);
});
// $("body").ready(function() {
// 	$(".progress-bar").animate({'width': '80%'},50);
// });
$("img").ready(function() {
            $(".progress-bar").animate({'width': '80%'},50);
})
$("script").ready(function() {
	$(".progress-bar").animate({'width': '90%'},50);
});
$(document).ready(function() {
	//进度条加载完即消失
	$(".loading").fadeOut();
            var changeWidth = function() {
                         var windowHeight = $(window).height(),
                               slideWidth = windowHeight*(750/1181),
                               windowWidth = $(window).width();
                              if(slideWidth < windowWidth) {
                                     $('.fb-tap').css('width',slideWidth*0.5);
                                     $('#introduce-slide').css('width',slideWidth*0.8)
                                     $('#football-tap').css('width',slideWidth*0.54);
                                     $('#fb-content').css('width',slideWidth);
                                     $('#food-content').css('width',slideWidth);
                                     $('#car-content').css('width',slideWidth);
                                     $('#play-content').css('width',slideWidth);
                                     $('#horse-content').css('width',slideWidth);                              
                          }     

            }
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
            changeWidth();             
             $(window).on('resize',changeWidth); 
}); 

// 当前屏幕的动画
