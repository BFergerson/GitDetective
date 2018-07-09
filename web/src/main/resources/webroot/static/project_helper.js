eb.onopen = function () {
  console.log('Connected to event bus')
  triggerJobLogTimer(githubRepository)

  var canTriggerBuild = false
  var canTriggerRecalculation = false
  eb.send('GetTriggerInformation', {'github_repo': githubRepository},
    function (error, message) {
      if (error == null) {
        canTriggerBuild = message.body.can_build
        canTriggerRecalculation = message.body.can_recalculate
        if (canTriggerBuild) {
          $('#trigger_build_button').removeClass('disabled')
        }
        if (canTriggerRecalculation) {
          $('#trigger_recalculate_button').removeClass('disabled')
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
        eb.send('CreateJob', {'github_repo': githubRepository},
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
        $('#trigger_recalculate_button').addClass('disabled')
      }
    })
    $('#trigger_recalculate_button').click(function () {
      if (canTriggerRecalculation) {
        canTriggerRecalculation = false
        eb.send('TriggerRecalculation', {'github_repo': githubRepository},
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
        $('#trigger_recalculate_button').addClass('disabled')
      }
    })
  })

  function triggerJobLogTimer (githubRepository) {
    //immediate run
    eb.send('GetLatestJobLog', {'github_repo': githubRepository},
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
      eb.send('GetLatestJobLog', {'github_repo': githubRepository},
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

  eb.send('GetMethodMethodReferences', {
    'github_repo': method.github_repo,
    'method_id': method.id,
    'offset': 0,
  }, function (error, message) {
    if (error == null) {
      $('#method_references').html('')
      var mostReferencedMethods = message.body
      for (var i = 0; i < mostReferencedMethods.length; i++) {
        var method = mostReferencedMethods[i]
        var codeLocation = 'https://github.com/' + method.github_repo +
          '/blob/' + method.commit_sha1 + '/' + method.file_location

        //table entry
        var rowHtml = '<tr>'
        rowHtml += '<td><h6>' + method.short_class_name +
          '</h6> <div style="max-width: 450px; word-wrap:break-word;" class="text-muted">'
          + method.short_method_signature.toHtmlEntities() +
          '</div></td>'
        rowHtml += '<td><button onclick=\'location.href="' + gitdetectiveUrl +
          method.github_repo +
          '";\' type="button" class="btn waves-effect waves-light btn-outline-primary">' +
          method.github_repo +
          '</button></td>'
        rowHtml += '<td><a target="_blank" href="' + codeLocation + '">' +
          '<button type="button" class="btn waves-effect waves-light btn-outline-primary">Code location</button>' +
          '</a></td>'
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

function displayMethodCopy (method) {
  $('#copied_methods_breadcrumbs').append(
    '<li id="most_copied_methods_breadcrumb" class="breadcrumb-item active">' +
    '<a>Method [id: ' + method.id + ']</a></li>')
  $('#most_copied_methods_breadcrumb').removeClass('active')
  $('#most_copied_methods_breadcrumb_link').attr('href', '#')

  $('#copies_main_title').
    html('<small class="text-muted"><b>Copied method</b>: ' +
      method.class_name + '.'
      + method.method_signature.toHtmlEntities() + '</small>')
  $('#most_copied_methods_table').hide()
  $('#method_copies_table').show()

  if (method.external_copy_count < 10) {
    $('#display_copy_amount_information').
      html('<b>Displaying ' + method.external_copy_count + ' of ' +
        method.external_copy_count + '</b>')
  } else {
    $('#display_copy_amount_information').
      html('<b>Displaying 10 of ' + method.external_copy_count + '</b>')
  }

  eb.send('GetMethodMethodCopies', {
    'github_repo': method.github_repo,
    'method_id': method.id,
    'offset': 0,
  }, function (error, message) {
    if (error == null) {
      $('#method_copies').html('')
      var mostCopiedMethods = message.body
      for (var i = 0; i < mostCopiedMethods.length; i++) {
        var method = mostCopiedMethods[i]
        var codeLocation = 'https://github.com/' + method.github_repo +
          '/blob/' + method.commit_sha1 + '/' + method.file_location

        //table entry
        var rowHtml = '<tr>'
        rowHtml += '<td><h6>' + method.short_class_name +
          '</h6> <div style="max-width: 450px; word-wrap:break-word;" class="text-muted">'
          + method.short_method_signature.toHtmlEntities() +
          '</div></td>'
        rowHtml += '<td><button onclick=\'location.href="' + gitdetectiveUrl +
          method.github_repo +
          '";\' type="button" class="btn waves-effect waves-light btn-outline-primary">' +
          method.github_repo +
          '</button></td>'
        rowHtml += '<td><a target="_blank" href="' + codeLocation + '">' +
          '<button type="button" class="btn waves-effect waves-light btn-outline-primary">Code location</button>' +
          '</a></td>'
        rowHtml += '</tr>'
        $('#method_copies').append(rowHtml)
      }

      $('#display_copy_amount_information').show()
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

function displayMostCopiedMethods () {
  $('#copies_main_title').html('')
  $('#most_copied_methods_table').show()
  $('#method_copies_table').hide()
  $('#most_copied_methods_breadcrumb').addClass('active')
  $('#most_copied_methods_breadcrumb_link').removeAttr('href')
  $('#display_copy_amount_information').hide()

  var listItems = $('#copied_methods_breadcrumbs li')
  if (listItems.length > 1) {
    listItems[listItems.length - 1].remove()
  }
}