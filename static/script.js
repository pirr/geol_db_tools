

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
	id_reg = button.id
	actualCheck = document.getElementById('actualCheckbox')
	if (actualCheck.checked) {
		wtype = 'actual'
	} else {
		wtype = 'work'
	}
	console.log(id_reg+wtype)
	// var OSName="pass";
	// if (navigator.appVersion.indexOf("Win")!=-1) OSName="Windows";
	$.ajax({
		type: 'GET',
		headers: { "cache-control": "no-cache" },
		url: '/get_download',
		cache: false,
		data: {id_reg: id_reg, wtype: wtype},
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
        	$('#loading_'+id_reg).html('Проблема при загрузке файла')
        },
		
		beforeSend: function() {
	        $('#loading_'+id_reg).show();
			$('#loading_'+id_reg).html('Файл формируется')
	    },
	   
		success: function() {
			$('#loading_'+id_reg).html('Файл загружается')
			window.location = '/download/'+ id_reg + '-' + wtype
			// $.get('/clear_session', {param: 'file_size'})
		}
	})
}