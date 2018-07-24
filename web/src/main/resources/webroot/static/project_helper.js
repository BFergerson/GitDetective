eb.onopen = function () {
  console.log('Connected to event bus')
  triggerJobLogTimer(githubRepository)

  var canTriggerBuild = false
  eb.send('GetTriggerInformation', {'github_repository': githubRepository},
    function (error, message) {
      if (error == null) {
        canTriggerBuild = message.body.can_build
        if (canTriggerBuild) {
          $('#trigger_build_button').removeClass('disabled')
        }
      } else {
        console.log(error.message)
        $('#latest_job_log').append(error.message)
      }
    })

  $(document).ready(function () {
    //buttons
    $('#trigger_build_button').click(function () {
      if (canTriggerBuild) {
        canTriggerBuild = false
        eb.send('CreateJob', {'github_repository': githubRepository},
          function (error, message) {
            if (error == null) {
              $('#latest_job_log').text('')
              logPositionIndex = -1
              triggerJobLogTimer(githubRepository)
            } else {
              console.log(error.message)
              $('#latest_job_log').append(error.message)
            }
          })
        $('#trigger_build_button').addClass('disabled')
      }
    })
  })

  function triggerJobLogTimer (githubRepository) {
    //immediate run
    eb.send('GetLatestJobLog', {'github_repository': githubRepository},
      function (error, message) {
        if (error == null) {
          displayLatestJobLog(message.body)
        } else {
          console.log(error.message)
          $('#latest_job_log').append(error.message)
        }
      })

    //timer
    setInterval(function () {
      console.log('Started job log timer')
      eb.send('GetLatestJobLog', {'github_repository': githubRepository},
        function (error, message) {
          if (error == null) {
            displayLatestJobLog(message.body)
          } else {
            console.log(error.message)
            $('#latest_job_log').append(error.message)
          }
        })
    }, 7500)
  }

  function displayLatestJobLog (logs) {
    console.log('Displaying latest job logs')
    if (logs.job_id !== latestJobLogId) {
      $('#latest_job_log').text('')
      logPositionIndex = -1
      latestJobLogId = logs.job_id
    }

    for (var i = 0; i < logs.logs.length; i++) {
      if (logPositionIndex < i) {
        $('#latest_job_log').append(logs.logs[i] + '<br>')
        logPositionIndex = i
      }
    }
  }
}

function displayMethodReference (method) {
  $('#referenced_methods_breadcrumbs').append(
    '<li id="most_referenced_methods_breadcrumb" class="breadcrumb-item active">' +
    '<a>Method [id: ' + method.id + ']</a></li>')
  $('#most_referenced_methods_breadcrumb').removeClass('active')
  $('#most_referenced_methods_breadcrumb_link').attr('href', '#')

  $('#references_main_title').
    html('<small class="text-muted"><b>Referenced method</b>: ' +
      method.class_name + '.'
      + method.method_signature.toHtmlEntities() + '</small>')
  $('#most_referenced_methods_table').hide()
  $('#method_references_table').show()

  if (method.external_reference_count < 10) {
    $('#display_reference_amount_information').
      html('<b>Displaying ' + method.external_reference_count + ' of ' +
        method.external_reference_count + '</b>')
  } else {
    $('#display_reference_amount_information').
      html('<b>Displaying 10 of ' + method.external_reference_count + '</b>')
  }

  eb.send('GetMethodExternalReferences', {
    'github_repository': method.github_repository,
    'method_id': method.id,
    'offset': 0,
  }, function (error, message) {
    if (error == null) {
      $('#method_references').html('')
      var mostReferencedMethods = message.body
      for (var i = 0; i < mostReferencedMethods.length; i++) {
        var methodOrFile = mostReferencedMethods[i]
        var codeLocation = 'https://github.com/' + methodOrFile.github_repository +
          '/blob/' + methodOrFile.commit_sha1 + '/' + methodOrFile.file_location

        //table entry
        var rowHtml = '<tr>'
        if (methodOrFile.is_function) {
          rowHtml += '<td><h6>' + methodOrFile.short_class_name +
            '</h6> <div style="max-width: 450px; word-wrap:break-word;" class="text-muted">'
            + methodOrFile.short_method_signature.toHtmlEntities() +
            '</div></td>'
          rowHtml += '<td><button onclick=\'location.href="' + gitdetectiveUrl +
            methodOrFile.github_repository +
            '";\' type="button" class="btn waves-effect waves-light btn-outline-primary">' +
            methodOrFile.github_repository +
            '</button></td>'
          rowHtml += '<td><a target="_blank" href="' + codeLocation + '">' +
            '<button type="button" class="btn waves-effect waves-light btn-outline-primary">Code location</button>' +
            '</a></td>'
        } else {
          rowHtml += '<td><h6>' + methodOrFile.short_class_name +
            '</h6> <div style="max-width: 450px; word-wrap:break-word;" class="text-muted">'
            + methodOrFile.file_location + '</div></td>'
          rowHtml += '<td><button onclick=\'location.href="' + gitdetectiveUrl +
            methodOrFile.github_repository +
            '";\' type="button" class="btn waves-effect waves-light btn-outline-primary">' +
            methodOrFile.github_repository +
            '</button></td>'
          rowHtml += '<td><a target="_blank" href="' + codeLocation + '">' +
            '<button type="button" class="btn waves-effect waves-light btn-outline-primary">Code location</button>' +
            '</a></td>'
        }
        rowHtml += '</tr>'
        $('#method_references').append(rowHtml)
      }

      $('#display_reference_amount_information').show()
    } else {
      console.log(error.message)
      $('#latest_job_log').append(error.message)
    }
  })
}

function displayMostReferencedMethods () {
  $('#references_main_title').html('')
  $('#most_referenced_methods_table').show()
  $('#method_references_table').hide()
  $('#most_referenced_methods_breadcrumb').addClass('active')
  $('#most_referenced_methods_breadcrumb_link').removeAttr('href')
  $('#display_reference_amount_information').hide()

  var listItems = $('#referenced_methods_breadcrumbs li')
  if (listItems.length > 1) {
    listItems[listItems.length - 1].remove()
  }
}
