(function () {
	// Create the connector object
	var myConnector = tableau.makeConnector();


	// Define the schema
	myConnector.getSchema = function (schemaCallback) {
		var inputForm = JSON.parse(tableau.connectionData),
			callbackTables = [];

		var dealsCols = [
			{ id: 'id', dataType: tableau.dataTypeEnum.int },
			{ id: '62b493b099a8a35016ad9a200703cfaf58877f0c',alias:"ID_SHOP", dataType: tableau.dataTypeEnum.string },
			{ id: '6ef8247fb14d02c6664a2aed26d06353a07b8181',alias:"Vertical", dataType: tableau.dataTypeEnum.string },
			{ id: 'd9de4755ac5941ad1c1f6025a2da7fab85ebee64',alias:"Kategorie", dataType: tableau.dataTypeEnum.string },
			{ id: 'ea19588a42c3f32b745e459439d3464c00629a64',alias:"Qualitätsmerkmale", dataType: tableau.dataTypeEnum.string },
			{ id: 'f344ad4fd97d701de9185a9a6dba5c2df9bf3111',alias:"Hauptkategorie", dataType: tableau.dataTypeEnum.int },
			{ id: 'owner_name', dataType: tableau.dataTypeEnum.string },
			{ id: 'title', dataType: tableau.dataTypeEnum.string },
			{ id: 'status', dataType: tableau.dataTypeEnum.string },
			{ id: 'won_time', dataType: tableau.dataTypeEnum.string },
			{ id: 'lost_time', dataType: tableau.dataTypeEnum.string },
			{ id: 'deleted', dataType: tableau.dataTypeEnum.string  },
			{ id: 'close_time', dataType: tableau.dataTypeEnum.string  },
			{ id: 'add_time', dataType: tableau.dataTypeEnum.date },
			{ id: 'stage_order_nr', dataType: tableau.dataTypeEnum.string },
			{ id: 'stage_id', dataType: tableau.dataTypeEnum.string },


		];

		// Schema for users
		var categoryCols = [
			{ id: 'id', dataType: tableau.dataTypeEnum.int },	
			{ id: 'label', dataType: tableau.dataTypeEnum.string },	
			
		]

		var dealsTable = { id: 'deals', alias: 'deals', columns: dealsCols };

		var categoryTable = { id: 'maincategory', alias: 'dealFields/12551', columns: categoryCols };


		if (inputForm.tables.indexOf('deals') >= 0) callbackTables.push(dealsTable);



		schemaCallback(callbackTables);
	};

	// Download the data
	myConnector.getData = function (table, doneCallback) {
		var inputForm = JSON.parse(tableau.connectionData);

		var tableData = [];
		var len;
		var lenj;
		var i = 0;
		var j = 0;
		var r = 0;
		var start = 0;
		var limit = 500;
			
		function formatURL(table, api_token, start, limit, start_date, end_date) {
			var url = 'https://api.pipedrive.com/v1/' + table + '?api_token=4b428bd92bbb998eab97f3beeaa183d40b472552'  + '&start=' + start + '&limit=' + limit;
			if(inputForm.startDate !== null) url = url + '&start_date=' + inputForm.startDate;
			if(inputForm.endDate !== null) url = url + '&end_date=' + inputForm.endDate;
			return url;
		}

		if (inputForm.tables.indexOf(table.tableInfo.id) >= 0) {
			var hasMore = true;

			while (hasMore) {
				$.ajax({
					url: formatURL(table.tableInfo.alias, inputForm.apiKey, start, limit, inputForm.startDate, inputForm.endDate),
					dataType: 'json',
					async: false,
					success: function(resp) {
						r = r + 1;
						if (r > 100) return;

						hasMore = false;

						// This is the payload we care about
						var _data = resp.data;
                        if (table.tableInfo.id === 'maincategory' || table.tableInfo.id === 'quality') {
						    var _data = resp.data.options;}
						// This is pagination data (if available)
						var extra = resp.additional_data;

						if (extra && extra.pagination.more_items_in_collection) {
							hasMore = true;
							start = extra.pagination.next_start;
							tableau.log('Has more rows, next start at ' + start);
						}

						for (i = 0, len = _data.length; i < len; i++) {
							var ndata = $.extend({}, _data[i]);

							if (table.tableInfo.id === 'activities') {
								var participants = _data[i].participants;
								if (participants) {
									ndata.participant_count = participants.length;
									ndata.participants = '';
									for (j = 0, lenj = _data[i].participants.length; j < lenj; j++) {
										ndata.participants = ndata.participants + _data[i].participants[j].person_id;
									}
									ndata.participants = ndata.participants.slice(0, -1);
								}
							}
							
							if (table.tableInfo.id == 'persons') {
								if (_data[i].phone) {
									var home_phone = _data[i].phone.filter(function (phone) {
										return phone.label === 'home';
									});
									var work_phone = _data[i].phone.filter(function (phone) {
										return phone.label === 'work';
									});
									var mobile_phone = _data[i].phone.filter(function (phone) {
										return phone.label === 'mobile';
									});
									var other_phone = _data[i].phone.filter(function (phone) {
										return phone.label === 'other';
									});

									ndata.home_phone = home_phone.length > 0 ? home_phone[0].value : '';
									ndata.work_phone = work_phone.length > 0 ? work_phone[0].value : '';
									ndata.mobile_phone = mobile_phone.length > 0 ? mobile_phone[0].value : '';
									ndata.other_phone = other_phone.length > 0 ? other_phone[0].value : '';
								}
								if (_data[i].email) {
									var home_email = _data[i].email.filter(function (email) {
										return email.label === 'home';
									});
									var work_email = _data[i].email.filter(function (email) {
										return email.label === 'work';
									});
									var other_email = _data[i].email.filter(function (email) {
										return email.label === 'other';
									});

									ndata.home_email = home_email.length > 0 ? home_email[0].value : '';
									ndata.work_email = work_email.length > 0 ? work_email[0].value : '';
									ndata.other_email = other_email.length > 0 ? other_email[0].value : '';
								}

								ndata.has_im = (_data[i].im[0].value === '' ? false : true);

								// Need to add IM, but this is getting exhausting... homework for the first person that needs it.
							}
							if (table.tableInfo.id == 'products') {
								// At least one record, more if there are variations
								ndata.owner_id = _data[i].owner_id.id;
								ndata.price_amount = _data[i].prices[0].price;
								ndata.price_currency = _data[i].prices[0].currency;
								ndata.price_cost = _data[i].prices[0].cost;

								if (_data[i].product_variations) {
									for (j = 0, lenj = _data[i].product_variations.length; j < lenj; j++) {
										var mdata = $.extend({}, ndata);

										mdata.variation_id = _data[i].product_variations[j].id;
										mdata.variation_name = _data[i].product_variations[j].name;
										mdata.variation_price = _data[i].product_variations[j].prices[0].price;
										mdata.variation_commennt = _data[i].product_variations[j].prices[0].comment;

										tableData.push(mdata);
									}
								}
								else {
									tableData.push(ndata);
								}
							}
                            

							else {
								tableData.push(ndata);
							}
						}

						table.appendRows(tableData);
					},
          error: function(resp) {
            hasMore = false;
          },
				});
			}
		}

		doneCallback();
	};

	tableau.registerConnector(myConnector);

	// Create event listeners for when the user submits the form
	$(document).ready(function () {
		$('#submitButton').click(function () {
			var inputForm = {
				apiKey: $('#api-key').val().trim(),
				startDate: $('#start-date').val().trim(),
				endDate: $('#end-date').val().trim(),
				tables: [],
			};

			if ($('#activities').is(':checked')) inputForm.tables.push('activities');
			if ($('#deals').is(':checked')) inputForm.tables.push('deals');
			if ($('#goals').is(':checked')) inputForm.tables.push('goals');
			if ($('#notes').is(':checked')) inputForm.tables.push('notes');
			if ($('#organizations').is(':checked')) inputForm.tables.push('organizations');
			if ($('#persons').is(':checked')) inputForm.tables.push('persons');
			if ($('#products').is(':checked')) inputForm.tables.push('products');
			if ($('#stages').is(':checked')) inputForm.tables.push('stages');
			if ($('#users').is(':checked')) inputForm.tables.push('users');
			inputForm.tables.push('maincategory');


			if (inputForm.startDate === '') inputForm.startDate = null;
			if (inputForm.endDate === '') inputForm.endDate = null;
			if (inputForm.startDate !== null) inputForm.startDate = new Date(inputForm.startDate);
			if (inputForm.endDate !== null) inputForm.endDate = new Date(inputForm.endDate);

			function isValid(form) {
			  /*
			   * 0 = OK
         * 1 = invalid date format
         * 2 = start date is after end date
         * 3 = no API key
         */
				if ((form.startDate !== null && form.startDate.toString() === 'Invalid Date')
          || (form.endDate !== null && form.endDate.toString() === 'Invalid Date'))
					return 1;

				if (form.startDate !== null && form.endDate !== null) {
					// Check that start is earlier than finish
					if (form.startDate > form.endDate) return 2;
				}

				if(form.apiKey.length !== 40) return 3;

				return 0;
			}

			// Need validation?
      var valid = isValid(inputForm);
			if (valid === 0) {
				tableau.connectionData = JSON.stringify(inputForm); // Use this variable to pass data to your getSchema and getData functions
				tableau.connectionName = 'Pipedrive'; // This will be the data source name in Tableau
				tableau.submit(); // This sends the connector object to Tableau
			} else {
				if(valid === 1) $('#errorMsg').html('<strong>Invalid Date entered</strong>');
				else if(valid === 2) $('#errorMsg').html('<strong>End date must be after start date</strong>');
				else if(valid === 3) $('#errorMsg').html('<strong>Missing or incorrect API key</strong>');
			}
		});
	});
})();
