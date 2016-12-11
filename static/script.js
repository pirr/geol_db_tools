

$(function(){
	
	// navbar active setup
	$('li.active').removeClass('active');
  	$('a[href="' + location.pathname + '"]').closest('li').addClass('active'); 

	// var progressElem = $('#progressCounter');
	// $("#loading").hide();
	// progressElem.text('progress');
	// $('#upload_form_div').load('/upload')
	// $()
	// $('#upload_form').bind('ajax:complete', function(html) {
	// 	// e.preventDefault();
	// 	console.log('after submit')
	// 	$('#upload_form_div').html(html)

	// })
	// load_form('/upload', '#upload_form');
// });


// var load_form = function(url, div_id){
// 	$.ajax({
// 		url: url,
// 		success: function(html) {
// 			$(div_id).load(html)
// 		}
// 	})
})

var downloadDB = function(button) {
	console.log(button.id)
	// $.get('/clear_session', {param: 'file_size'});
	reg_name = button.id
	// var OSName="pass";
	// if (navigator.appVersion.indexOf("Win")!=-1) OSName="Windows";
	$.ajax({
		type: 'GET',
		headers: { "cache-control": "no-cache" },
		url: '/get_download',
		cache: false,
		data: {reg_name: reg_name, with_revs: true},
		// xhr: function() {
		// 	var xhr = new XMLHttpRequest();

		// 	xhr.addEventListener('progress', function (evt) {
		// 		// evt.lengthComputable = true
		//         console.log(evt.lengthComputable); // false
		//         if (evt.lengthComputable) {
		//             var percentComplete = evt.loaded / evt.total;
		//             console.log(percentComplete)
		//             progressElem.html(Math.round(percentComplete * 100) + "%");
		//         }
		//     }, false);
		//     return xhr;
		// },
		// xhrFields: {
  //               withCredentials: true
  //       },
        
        error: function() {
        	$('#loading_'+reg_name).html('Проблема при загрузке файла')
        },
		
		beforeSend: function() {
	        $('#loading_'+reg_name).show();
	    },
	   
		success: function() {
			$('#loading_'+reg_name).html('Файл загружается')
			window.location = '/download/'+ reg_name
			// $.get('/clear_session', {param: 'file_size'})
		}
	})
}