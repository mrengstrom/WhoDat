$(document).ready(function(){

    $('#dnsdb-tab').tabs();
    $('.dnsdb-dnsres').dataTable(
        {
            "bJQueryUI": true,
            //"bAutoWidth": false,
            "oLanguage": {'sSearch': 'Filter:',
                            'sZeroRecords': 'No Records Found',
                            },
            "aoColumnDefs": [
                {"sWidth": "15%", 'bSortable': false, 'aTargets': [-1]},
            ],
            "sPaginationType": "full_numbers",
            "sDom" : '<"H"lfirp>t<"F"lfip>',
            "aLengthMenu": [[50, 100, 200, 300], [50, 100, 200, 300]],
            "iDisplayLength" : 100,
            "fnDrawCallback": function(oSettings) {
                $(".resolve").on("click", function() {
                    $(this).removeClass('link');
                    $(this).off('click');
                    resolve($(this).attr('domainName'), $(this));
                });
            }
        });

    $(".pdns_search_form [name='result_format']").change(function(){
        //Filtering should only be visible for List
        if($(this).find("option:selected").val() == "list"){
            $(this).parents().find('.form_filter').removeClass("novis").show();
        }else{
            $(this).parents().find('.form_filter').addClass("novis");
        }
    });


    $(".pdns_search_form [name='result_format']").change();


});
