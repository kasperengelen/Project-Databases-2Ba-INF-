from flask import Blueprint, url_for, redirect, flash, abort, request
from View.transf_forms import DataTypeTransform, NormalizeZScore, OneHotEncoding, RegexFindReplace, DiscretizeEqualWidth, ExtractDateTimeForm
from View.transf_forms import DiscretizeEqualFreq, DiscretizeCustomRange, DeleteOutlier, FillNullsMean, FillNullsMedian, FillNullsCustomValue
from View.transf_forms import PredicateFormOne, PredicateFormTwo, PredicateFormThree, FindReplaceForm, DedupForm
from Controller.TableTransformer import TableTransformer
from Controller.AccessController import require_login, require_admin
from Controller.AccessController import require_adminperm, require_writeperm, require_readperm
from Controller.DatasetManager import DatasetManager
from View.form_utils import flash_errors

transf_callbacks = Blueprint('transf_callbacks', __name__)

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/transform_predicate', methods=['POST'])
@require_login
@require_writeperm
def transform_predicate(dataset_id, tablename):
    """Callback for delete by predicate"""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    predicateone = PredicateFormOne(request.form)
    predicateone.fillForm(tv.get_attributes())
    predicateone.handle_tt(tt) # handle tabletransformer
    predicatetwo = PredicateFormTwo(request.form)
    predicatetwo.fillForm(tv.get_attributes())
    predicatethree = PredicateFormThree(request.form)
    predicatethree.fillForm(tv.get_attributes())

    if not predicateone.validate():
        flash_errors(predicateone)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    predicate_list = [predicateone.attr1.data, predicateone.op1.data, predicateone.input1.data]

    if(predicateone.select1.data != "END"):
        if not predicatetwo.validate():
            flash_errors(predicatetwo)
            return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))    

        predicate_list.append(predicateone.select1.data)
        predicate_list.append(predicatetwo.attr2.data)
        predicate_list.append(predicatetwo.op2.data)
        predicate_list.append(predicatetwo.input2.data)

    if(predicatetwo.select2.data != "END"):
        if not predicatethree.validate():
            flash_errors(predicatethree)
            return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

        predicate_list.append(predicatetwo.select2.data)
        predicate_list.append(predicatethree.attr3.data)
        predicate_list.append(predicatethree.op3.data)
        predicate_list.append(predicatethree.input3.data)
    
    try:
        tt.delete_rows_using_predicate_logic(tablename=tablename, arg_list=predicate_list, new_name = predicateone.new_table_name.data)
        flash(message="Rows deleted according to predicate.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=predicateone.get_table_name(tablename)))
    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/transform_extractdatetime', methods=['POST'])
@require_login
@require_writeperm
def transform_extractdatetime(dataset_id, tablename):
    """Callback for extracting date/time"""
    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = ExtractDateTimeForm(request.form)
    form.fillForm(tv.get_attributes())
    form.handle_tt(tt)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    try:
        tt.extract_part_of_date(tablename, form.select_attr.data, form.select_extracttype.data, new_name=form.new_table_name.data)
        flash(message="Part of date extracted.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=form.get_table_name(tablename)))

    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/deleteattr', methods=['POST'])
@require_login
@require_writeperm
def transform_deleteattr(dataset_id, tablename):
    """Callback for delete attribute transformation."""

    attrname = request.form.get('attribute_name', type=str)

    print(attrname)

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)
    
    tv = dataset.getTableViewer(tablename)

    if not attrname in tv.get_attributes():
        flash(message="Invalid attribute.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    tt = dataset.getTableTransformer(tablename)
    try:
        tt.delete_attribute(tablename, attrname)
        flash(message="Attribute deleted.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/findreplace', methods=['POST'])
@require_login
@require_writeperm
def transform_findreplace(dataset_id, tablename):
    """Callback for universal search and replace transformation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = FindReplaceForm(request.form)
    form.fillForm(tv.get_attributes())
    form.handle_tt(tt)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    try:
        tt.find_and_replace(tablename   = tablename, 
                            attribute   = form.select_attr.data, 
                            value       = form.search.data, 
                            replacement = form.replacement.data, 
                            exact       = form.exactmatch.data,
                            replace_all = form.replace_full_match.data, 
                            new_name    = form.new_table_name.data)
        flash(message="Find and replace completed.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=form.get_table_name(tablename)))
    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/regexfindreplace', methods=['POST'])
@require_login
@require_writeperm
def transform_findreplaceregex(dataset_id, tablename):
    """Callback for the regex find replace transformation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = RegexFindReplace(request.form)
    form.fillForm(tv.get_attributes())
    form.handle_tt(tt)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    try:
        tt.regex_find_and_replace(tablename   = tablename, 
                                  attribute   = form.select_attr.data, 
                                  regex       = form.regex.data, 
                                  replacement = form.replacement.data, 
                                  case_sens   = form.case_sens.data,
                                  new_name    = form.new_table_name.data)
        flash(message="Find and replace complete.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=form.get_table_name(tablename)))
    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/typeconversion', methods = ['POST'])
@require_login
@require_writeperm
def transform_typeconversion(dataset_id, tablename):
    """Callback for typeconversion transformation."""

    # input checks
    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tt = dataset.getTableTransformer(tablename)
    tv = dataset.getTableViewer(tablename)

    form = DataTypeTransform(request.form)

    # prepare formats
    datetimetypes = []
    datetimetypes.append("None")
    for datetype in tt.get_datetime_formats("DATE"):
        datetimetypes.append(datetype) 

    for datetype in tt.get_datetime_formats("TIME"):
        datetimetypes.append(datetype)

    for datetype in tt.get_datetime_formats("TIMESTAMP"):
        datetimetypes.append(datetype)

    # TODO change this
    form.fillForm(tv.get_attributes(), tt.get_conversion_options(tablename, form.select_attr.data), datetimetypes)
    form.handle_tt(tt)

    # validate form
    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    # retrieve force attribute information

    do_force = form.do_force.data
    force_mode = form.force_mode.data

    if do_force:
        try:
            # set none
            if form.new_datatype.data == 'VARCHAR(255)':
                form.char_amount.data = 255
            elif form.new_datatype.data not in ['VARCHAR(n)', 'CHAR(n)']:
                form.char_amount.data = None

            tt.force_attribute_type(tablename   = tablename, 
                                    attribute   = form.select_attr.data, 
                                    to_type     = form.new_datatype.data, 
                                    data_format = form.date_type.data,
                                    length      = form.char_amount.data,
                                    force_mode  = force_mode,
                                    new_name    = form.new_table_name.data)
            flash(message="Attribute type changed.", category="success")
            redir_name = form.get_table_name(tablename)
        except (TableTransformer.TTError) as e:
            flash(message=str(e), category="error")
            redir_name = tablename
    else:
        try:
            tt.change_attribute_type(tablename   = tablename, 
                                     attribute   = form.select_attr.data, 
                                     to_type     = form.new_datatype.data, 
                                     data_format = form.date_type.data, 
                                     length      = form.char_amount.data,
                                     new_name    = form.new_table_name.data)
            flash(message="Attribute type changed.", category="success")
            redir_name = form.get_table_name(tablename)
        except (TableTransformer.TTError) as e:
            flash(message=str(e), category="error")
            redir_name = tablename

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=redir_name))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/onehotencoding', methods = ['POST'])
@require_login
@require_writeperm
def transform_onehotencoding(dataset_id, tablename):
    """Callback for one hot encoding transformation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = OneHotEncoding(request.form)
    form.fillForm(tv.get_attributes())

    form.handle_tt(tt)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    try:
        tt.one_hot_encode(tablename = tablename, 
                          attribute = form.select_attr.data, 
                          new_name  = form.new_table_name.data)
        flash(message="One hot encoding complete.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=form.get_table_name(tablename)))
    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/zscorenormalisation', methods = ['POST'])
@require_login
@require_writeperm
def transform_zscorenormalisation(dataset_id, tablename):
    """Callback for z-score normalisation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tt = dataset.getTableTransformer(tablename)
    tv = dataset.getTableViewer(tablename)

    form = NormalizeZScore(request.form)
    form.fillForm(tv.get_attributes())
    form.handle_tt(tt)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    try:
        tt.normalize_using_zscore(tablename = tablename, 
                                  attribute = form.select_attr.data, 
                                  new_name  = form.new_table_name.data)
        flash(message="Normalization complete.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=form.get_table_name(tablename)))
    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/discretize/equalwidth', methods = ['POST'])
@require_login
@require_writeperm
def transform_discretizeEqualWidth(dataset_id, tablename):
    """Callback for equal width discretization."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = DiscretizeEqualWidth(request.form)
    form.fillForm(tv.get_attributes())
    form.handle_tt(tt)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    try:
        tt.discretize_using_equal_width(tablename=tablename, 
                                        attribute=form.select_attr.data, 
                                        nr_bins=form.nr_of_bins.data, 
                                        new_name  = form.new_table_name.data)
        flash(message="Discretization complete.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=form.get_table_name(tablename)))
    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/discretize/equalfreq', methods = ['POST'])
@require_login
@require_writeperm
def transform_discretizeEqualFreq(dataset_id, tablename):
    """Callback for equal frequency discretization."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = DiscretizeEqualFreq(request.form)
    form.fillForm(tv.get_attributes())
    form.handle_tt(tt)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    try:
        tt.discretize_using_equal_frequency(tablename=tablename, 
                                            attribute=form.select_attr.data,
                                            new_name = form.new_table_name.data)
        flash(message="Discretization complete.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=form.get_table_name(tablename)))
    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/discretize/customrange', methods = ['POST'])
@require_login
@require_writeperm
def transform_discretizeCustomRange(dataset_id, tablename):
    """Callback to discretize the data into custom intervals."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = DiscretizeCustomRange(request.form)
    form.fillForm(tv.get_attributes())
    form.handle_tt(tt)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    # determine intervals
    split = form.ranges.data.split(',')

    int_ranges = []

    # convert to integer list
    for i in split:
        try:
            int_ranges.append(int(i))
        except:
            flash(message="Invalid range specifier: " + str(i), category="error")
            return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
    # ENDFOR

    # check if interval is in correct order and no empty bins
    if not all(int_ranges[i] < int_ranges[i+1] for i in range(0, len(int_ranges)-1)):
        flash(message="Range specifiers not in correct order or empty range detected.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    if len(int_ranges) < 2:
        flash(message="At least two range specifiers needed.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    try:
        tt.discretize_using_custom_ranges(tablename=tablename, 
                                          attribute=form.select_attr.data, 
                                          ranges=int_ranges, 
                                          exclude_right=form.interval_spec.data,
                                          new_name = form.new_table_name.data)
        flash(message="Discretization complete.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=form.get_table_name(tablename)))
    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/delete_outlier', methods = ['POST'])
@require_login
@require_writeperm
def transform_deleteOutlier(dataset_id, tablename):
    """Callback for transformation to delete outlying values."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = DeleteOutlier(request.form)
    form.fillForm(tv.get_attributes())
    form.handle_tt(tt)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    try:
        tt.delete_outliers(tablename=tablename, 
                            attribute=form.select_attr.data, 
                            larger=form.select_comparison.data, 
                            value=form.comparison_value.data,
                            replacement=form.replacement_value.data,
                            new_name = form.new_table_name.data)
        flash(message="Outliers deleted.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=form.get_table_name(tablename)))
    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/fill_null/mean', methods = ['POST'])
@require_login
@require_writeperm
def transform_fillNullsMean(dataset_id, tablename):
    """Callback for the fill nulls with mean transformation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = FillNullsMean(request.form)
    form.fillForm(tv.get_attributes())
    form.handle_tt(tt)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    try:
        tt.fill_nulls_with_mean(tablename=tablename, 
                                attribute=form.select_attr.data, 
                                new_name = form.new_table_name.data)
        flash(message="NULL values replaced with mean.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=form.get_table_name(tablename)))
    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/fill_null/median', methods = ['POST'])
@require_login
@require_writeperm
def transform_fillNullsMedian(dataset_id, tablename):
    """Callback for the fill nulls with median transformation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = FillNullsMedian(request.form)
    form.fillForm(tv.get_attributes())
    form.handle_tt(tt)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    try:
        tt.fill_nulls_with_median(tablename=tablename, 
                                  attribute=form.select_attr.data, 
                                  new_name = form.new_table_name.data)
        flash(message="NULL values replaced with median.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=form.get_table_name(tablename)))
    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/table/<string:tablename>/transform/fill_null/custom', methods = ['POST'])
@require_login
@require_writeperm
def transform_fillNullsCustomValue(dataset_id, tablename):
    """Callback for the fill nulls with median transformation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = FillNullsCustomValue(request.form)
    form.fillForm(tv.get_attributes())
    form.handle_tt(tt)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))

    try:
        tt.fill_nulls_with_custom_value(tablename=tablename, 
                                        attribute=form.select_attr.data, 
                                        value=form.replacement.data,
                                        new_name=form.new_table_name.data)
        flash(message="NULL values filled with custom value.", category="success")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=form.get_table_name(tablename)))
    except (TableTransformer.TTError) as e:
        flash(message=str(e), category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
# ENDFUNCTION
