from flask import Blueprint, url_for, redirect, flash, abort
from View.transf_forms import DataTypeTransform, NormalizeZScore, OneHotEncoding, RegexFindReplace, DiscretizeEqualWidth, ExtractDateTimeForm
from View.transf_forms import DiscretizeEqualFreq, DiscretizeCustomRange, DeleteOutlier, FillNullsMean, FillNullsMedian, FillNullsCustomValue
from View.transf_forms import PredicateFormOne, PredicateFormTwo, PredicateFormThree, FindReplaceForm, DeleteAttrForm
from Controller.TableTransformer import TableTransformer
from Controller.AccessController import require_login, require_admin
from Controller.AccessController import require_adminperm, require_writeperm, require_readperm

transf_callbacks = Blueprint('transf_callbacks', __name__)

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/transform_predicate', methods=['POST'])
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

    predicateone = PredicateFormOne(request.form)
    predicateone.fillForm(tv.get_attributes())
    predicatetwo = PredicateFormTwo(request.form)
    predicatetwo.fillForm(tv.get_attributes())
    predicatethree = PredicateFormThree(request.form)
    predicatethree.fillForm(tv.get_attributes())

    if not predicateone.validate():
        print(predicateone.errors)
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    predicate_list = [predicateone.attr1.data, predicateone.op1.data, predicateone.input1.data]

    if(predicateone.select1.data != "END"):
        if not predicatetwo.validate():
            print(predicatetwo.errors)
            flash(message="Invalid form.", category="error")
            return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))    

        predicate_list.append(predicateone.select1.data)
        predicate_list.append(predicatetwo.attr2.data)
        predicate_list.append(predicatetwo.op2.data)
        predicate_list.append(predicatetwo.input2.data)

    if(predicatetwo.select2.data != "END"):
        if not predicatethree.validate():
            print(predicatethree.errors)
            flash(message="Invalid form.", category="error")
            return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

        predicate_list.append(predicatetwo.select2.data)
        predicate_list.append(predicatethree.attr3.data)
        predicate_list.append(predicatethree.op3.data)
        predicate_list.append(predicatethree.input3.data)

    tt = dataset.getTableTransformer(tablename)
    try:
        tt.delete_rows_using_predicate_logic(tablename, predicate_list)
    except (TableTransformer.ValueError) as e:
        flash(message="An error occured. Details: " + str(e), category="error")
        
    flash(message="Rows deleted according to predicate.", category="success")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/transform_extractdatetime', methods=['POST'])
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

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    attr_type = tt.get_attribute_type(tablename, form.select_attr.data)[0]

    if tt.get_extraction_options(attr_type) == []:
        flash(message="Selected Attribute not of type 'DATE' or 'TIMESTAMP'.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    tt.extract_part_of_date(tablename, form.select_attr.data, form.select_extracttype.data)
    flash(message="Part of date extracted.", category="success")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/deleteattr', methods=['POST'])
@require_login
@require_writeperm
def transform_deleteattr(dataset_id, tablename):
    """Callback for delete attribute transformation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)

    form = DeleteAttrForm(request.form)
    form.fillForm(tv.get_attributes())

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    tt = dataset.getTableTransformer(tablename)

    tt.delete_attribute(tablename, form.select_attr.data)
    flash(message="Attribute deleted.", category="success")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/findreplace', methods=['POST'])
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

    form = FindReplaceForm(request.form)
    form.fillForm(tv.get_attributes())

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    tt = dataset.getTableTransformer(tablename)

    try:
        tt.find_and_replace(tablename, form.select_attr.data, form.search.data, form.replacement.data, form.exactmatch.data,
                            form.replace_full_match.data)
        flash(message="Find and replace completed.", category="success")
    except (TableTransformer.AttrTypeError, TableTransformer.ValueError) as e:
        flash(message="No matches found. Details: " + str(e), category="error")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/regexfindreplace', methods=['POST'])
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

    if not form.validate():
        print(form.errors)
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    try:
        tt.regex_find_and_replace(tablename, form.select_attr.data, form.regex.data, form.replacement.data, form.case_sens.data)
        flash(message="Find and replace complete.", category="success")
    except Exception as e:
        flash(message="An error occurred. Details: " + str(e), category="error")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/typeconversion', methods = ['POST'])
@require_login
@require_writeperm
def transform_typeconversion(dataset_id, tablename):
    """Callback for typeconversion transformation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = DataTypeTransform(request.form)
    print(form.new_datatype.data)

    datetimetypes = []
    datetimetypes.append("None")
    for datetype in tt.get_datetime_formats("DATE"):
        datetimetypes.append(datetype) 

    for datetype in tt.get_datetime_formats("TIME"):
        datetimetypes.append(datetype)

    for datetype in tt.get_datetime_formats("TIMESTAMP"):
        datetimetypes.append(datetype)   

    print(type(form.date_type.data))
    print(datetimetypes)

    form.fillForm(tv.get_attributes(), tt.get_conversion_options(tablename, form.select_attr.data), datetimetypes)


    if not form.validate():
        flash(message="Invalid form.", category="error")
        print(form.errors)
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    if not form.new_datatype.data in tt.get_conversion_options(tablename, form.select_attr.data):
        flash(message="Selected datatype not compatible with the selected attribute.", category="error")
    else:
        try:
            tt.change_attribute_type(tablename, form.select_attr.data, form.new_datatype.data, form.date_type.data, form.char_amount.data)
            flash(message="Attribute type changed.", category="success")
        except Exception as e:
            flash(message="An error occurred. Details: " + str(e), category="error")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/onehotencoding', methods = ['POST'])
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

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    try:
        tt.one_hot_encode(tablename, form.select_attr.data)
        flash(message="One hot encoding complete.", category="success")
    except Exception as e:
        flash(message="An error occurred. Details: " + str(e), category="error")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/zscorenormalisation', methods = ['POST'])
@require_login
@require_writeperm
def transform_zscorenormalisation(dataset_id, tablename):
    """Callback for z-score normalisation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = NormalizeZScore(request.form)
    form.fillForm(tv.get_attributes())

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    try:
        tt.normalize_using_zscore(tablename, form.select_attr.data)
        flash(message="Normalization complete.", category="success")
    except Exception as e:
        flash(message="An error occurred. Details: " + str(e), category="error")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/discretize/equalwidth', methods = ['POST'])
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

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    try:
        tt.discretize_using_equal_width(tablename, form.select_attr.data)
        flash(message="Discretization complete.", category="success")
    except Exception as e:
        flash(message="An error occurred. Details: " + str(e), category="error")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/discretize/equalfreq', methods = ['POST'])
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

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    try:
        tt.discretize_using_equal_frequency(tablename, form.select_attr.data)
        flash(message="Discretization complete.", category="success")
    except Exception as e:
        flash(message="An error occurred. Details: " + str(e), category="error")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/discretize/customrange', methods = ['POST'])
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

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    # determine intervals
    split = form.ranges.data.split(',')

    int_ranges = []

    # convert to integer list
    for i in split:
        try:
            int_ranges.append(int(i))
        except:
            flash(message="Invalid range specifier: " + str(i), category="error")
            return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
    # ENDFOR

    # check if interval is in correct order and no empty bins
    if not all(int_ranges[i] < int_ranges[i+1] for i in range(0, len(int_ranges)-1)):
        flash(message="Range specifiers not in correct order or empty range detected.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    if len(int_ranges) < 2:
        flash(message="At least two range specifiers needed.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    try:
        tt.discretize_using_custom_ranges(tablename, form.select_attr.data, int_ranges, form.interval_spec.data)
        flash(message="Discretization complete.", category="success")
    except Exception as e:
        flash(message="An error occurred. Details: " + str(e), category="error")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/delete_outlier', methods = ['POST'])
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

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    try:
        tt.delete_outlier(tablename, form.select_attr.data, form.select_comparison.data, form.value.data)
        flash(message="Outliers deleted.", category="success")
    except Exception as e:
        flash(message="An error occurred. Details: " + str(e), category="error")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/fill_null/mean', methods = ['POST'])
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

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    try:
        tt.fill_nulls_with_mean(tablename, form.select_attr.data)
        flash(message="NULL values replaced with mean.", category="success")
    except Exception as e:
        flash(message="An error occurred. Details: " + str(e), category="error")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/fill_null/median', methods = ['POST'])
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

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    try:
        tt.fill_nulls_with_median(tablename, form.select_attr.data)
        flash(message="NULL values replaced with median.", category="success")
    except Exception as e:
        flash(message="An error occurred. Details: " + str(e), category="error")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@transf_callbacks.route('/dataset/<int:dataset_id>/<string:tablename>/transform/fill_null/custom', methods = ['POST'])
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

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    try:
        tt.fill_nulls_with_custom_value(tablename, form.select_attr.data, form.replacement.data)
        flash(message="NULL values filled with custom value.", category="success")
    except Exception as e:
        flash(message="An error occurred. Details: " + str(e), category="error")

    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION
