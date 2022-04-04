from flask import Flask, render_template, request, session, redirect, url_for, flash
from werkzeug.utils import secure_filename
import os
from flask_cors import CORS, cross_origin
from functools import wraps
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from DbOperation.cassandraDB import cassandra_operation
from application_logging import logger

cassandra_obj=cassandra_operation() # Database operation object initialization
lg=logger.appLogger() #logger object initialization

#This scheduler for auto approve leave
try:
    scheduler1 = BackgroundScheduler()
    scheduler1.add_job(func=cassandra_obj.autoApproveLeave,  trigger='cron', hour='00', minute='10')
    scheduler1.start()
except Exception as e:
    lg.log('ERROR', f'scheduler1 failed : {e}')

#This scheduler for updating month_leave column by 2 at 1st day of every month
try:
    scheduler2=BackgroundScheduler()
    scheduler2.add_job(func=cassandra_obj.update_month_leave, trigger='cron', day='1', hour='00', minute='00')
    scheduler2.start()
except Exception as e:
    lg.log('ERROR', f'scheduler2 failed {e}')

#This scheduler  stores cash in cash column of database at last day of every year
try:
    scheduler3 = BackgroundScheduler()
    scheduler3.add_job(func=cassandra_obj.update_cash,  trigger='cron', month='12', day='31', hour='12', minute='00')
    scheduler3.start()
except Exception as e:
    lg.log('ERROR', f'scheduler3 failed : {e}')

#This scheduler updates total_leaves column in database by 24 at every last date of year
try:
    scheduler4 = BackgroundScheduler()
    scheduler4.add_job(func=cassandra_obj.update_total_leaves,  trigger='cron', month='12', day='31', hour='20', minute='00')
    scheduler4.start()
except Exception as e:
    lg.log('ERROR', f'scheduler4 failed : {e}')



app=Flask(__name__)
app.secret_key = "any random string"

#It checkes whether user logged in or not before any action
def login_required(f):
    try:
        @wraps(f)
        def wrap(*args, **kwargs):
            if 'logged_in' in session:
                return f(*args, **kwargs)
            else:
                return redirect(url_for('login'))
        return wrap
    except Exception as e:
        lg.log('ERROR', f'login_required failed : {e}')

#It show page not found if gets request for any unknown page
@app.errorhandler(404)
def not_found(e):
    return render_template("404.html")

@app.route('/', methods=['POST','GET'])
@cross_origin()
def index():
    return  redirect(url_for('login'))

@app.route('/login', methods=['POST','GET'])
@cross_origin()
def login():
        return render_template('login.html')

@app.route('/logout', methods=['POST','GET'])
@cross_origin()
@login_required
def logout():
        session.clear()
        return redirect(url_for('login'))

@app.route('/employee', methods=['POST','GET'])
@cross_origin()
@login_required
def employee():
    return render_template('employee.html')

@app.route('/admin', methods=['POST','GET'])
@cross_origin()
@login_required
def admin():
    return render_template('admin.html')

# It authenticates user after getting email id and password from login page
@app.route('/auth', methods=['POST','GET'])
@cross_origin()
def auth():
    try:
        if request.method == 'POST':
            email_id=request.form['email_id']
            password=request.form['password']
            output=cassandra_obj.login(email_id,password)
            if output is not None:
                session["email_id"] = email_id
                session["name"] = output['name']
                session["position"]=output['position']
                session['logged_in'] = True
                if output['position']=='Admin':
                    lg.log('INFO', 'login successfull')
                    return redirect(url_for('admin'))
                else:
                    lg.log('INFO', 'login successfull')
                    return redirect(url_for('employee'))
            else:
                flash('You have entered wrong Email Id or Password','danger')
                lg.log('INFO', 'login failed due to wrong info')
                return redirect(url_for('login'))

        else:
            lg.log('ERROR', 'login failed')
            return redirect(url_for('login'))


    except Exception as e:
        flash('Something went wrong', 'danger')
        lg.log('ERROR', f'login failed : {e}')
        return redirect(url_for('login'))




@app.route('/add-one', methods=['POST','GET'])
@cross_origin()
@login_required
def AddOneEmployee():
    try:
        if request.method == 'POST':
            name=request.form['name']
            email_id=request.form['email_id']
            contact_number=request.form['contact_number']
            dob=str(request.form['dob'])
            doj=str(request.form['doj'])
            position=request.form['position']
            salary=int(request.form['salary'])
            id=int(cassandra_obj.get_last_id())+1
            emp_id='iNeuron'+str(id)
            password=name.split()[0]+'@'+dob.split('-')[0]
            month_leave=2.0
            total_leaves=24.0

            info_list=[id, emp_id, name, email_id, contact_number, password, dob, doj, position, salary, month_leave, total_leaves]
            cassandra_obj.Add_One_Employee(info_list)
            flash('Employee added successfully', 'success')
            lg.log('INFO', 'Employee added successfully')
            return redirect(url_for('admin'))
    except Exception as e:
        flash('Something went wrong', 'danger')
        lg.log('ERROR', f'Failed to add employee : {e}')
        return redirect(url_for('admin'))


@app.route('/add-many', methods=['POST','GET'])
@cross_origin()
@login_required
def AddManyEmployee():
    try:
        if request.method == 'POST':
            f = request.files['filepath'] #takes the file input from form
            f.save(secure_filename(f.filename)) #saves the file in current directory
            cassandra_obj.Add_Many_Employee(f.filename)
            os.remove(f.filename) #delets the file from current directory
            flash('File uploaded successfully', 'success')
            lg.log('INFO', 'File uploaded successfully')
            return redirect(url_for('admin'))
    except Exception as e:
        os.remove(f.filename)  # delets the file from current directory
        lg.log('ERROR', f'Failed to upload file : {e}')
        flash('Something went wrong', 'danger')
        return redirect(url_for('admin'))

@app.route('/delete-employee', methods=['POST','GET'])
@cross_origin()
@login_required
def deleteEmployee():
    try:
        if request.method == 'POST':
            email_id=request.form['email_id']
            output=cassandra_obj.deleteEmployee(email_id)
            if output==True:
                flash('Employee deleted successfully', 'success')
                lg.log('INFO', 'Employee deleted succesfully')
                return redirect(url_for('admin'))
            else:
                flash("Email Id doesn't exsist",'warning')
                lg.log('INFO', 'Failed to delete employee due to wrong eamil id')
                return redirect(url_for('admin'))

    except Exception as e:
        flash('Something went wrong', 'danger')
        lg.log('ERROR', f'Failed to delete employee : {e}')
        return redirect(url_for('admin'))


@app.route('/edit', methods=['POST', 'GET'])
@cross_origin()
@login_required
def editEmployee():
    try:
        if request.method == 'POST':
            email_id=request.form['email_id']
            info=cassandra_obj.getInfo(email_id)
            if info==None:
                flash("Email Id doesn't exsist", 'warning')
                lg.log('INFO', 'Failed to return edit page due to wrong email id')
                return redirect(url_for('admin'))
            else:
                lg.log('INFO', 'edit page returned successfully')
                return render_template('edit.html', info=info)
    except Exception as e:
        flash('Something went wrong', 'danger')
        lg.log('ERROR', f'Failed to return edit page : {e}')
        return redirect(url_for('admin'))


@app.route('/updateEmployeeInfo', methods=['POST', 'GET'])
@cross_origin()
@login_required
def updateEmployeeInfo():
    try:
        if request.method == 'POST':
            name=request.form['name']
            email_id=request.form['email_id']
            contact_no=request.form['contact_no']
            dob=request.form['dob']
            join_date=request.form['join_date']
            position=request.form['position']
            salary=int(request.form['salary'])
            update_value_list=[name, email_id, contact_no, dob, join_date, position, salary]
            cassandra_obj.updateEmployeeInfo(update_value_list)
            flash('Emplyee info edited successfully','success' )
            lg.log('INFO', 'Employee info updated successfully')
            return redirect(url_for('admin'))
    except Exception as e:
        flash('Something went wrong', 'danger')
        lg.log('ERROR', f'Failed to update employee info : {e}')
        return redirect(url_for('admin'))


@app.route('/apply-leave', methods=['POST', 'GET'])
@cross_origin()
@login_required
def applyLeave():
    try:
        if request.method == 'POST':
            email_id=session.get('email_id')
            leave_date=request.form['leave_date']
            leave_type=float(request.form['leave_type'])
            leave_status='Pending'
            current_month=datetime.today().date().month
            leave_month=datetime.strptime(leave_date, '%Y-%m-%d').month
            if current_month==leave_month:
                available_monthly_leave=cassandra_obj.available_montly_leave(email_id)
                if (available_monthly_leave-leave_type)>0:
                    cassandra_obj.applyLeave(email_id, leave_date, leave_type, leave_status)
                    flash("Leave applied successfully", 'success')
                    lg.log('INFO', 'Leave applied successfully')
                    return  redirect(url_for('employee'))
                else:
                    flash(f"Leave can't be applied. You have {available_monthly_leave} leaves left for this month.", 'warning')
                    lg.log('INFO', 'Leave not applied due to non availability')
                    return redirect(url_for('employee'))
            else:
                cassandra_obj.applyLeave(email_id, leave_date, leave_type, leave_status)
                flash("Leave applied successfully", 'success')
                lg.log('INFO', 'Leave applied successfully')
                return redirect(url_for('employee'))

    except Exception as e:
        flash('Something went wrong', 'danger')
        lg.log('ERROR', f'Failed to apply leave : {e}')
        return redirect(url_for('employee'))

@app.route('/applied-leaves', methods=['POST', 'GET'])
@cross_origin()
@login_required
def appliedLeaves():
    try:
        pending_leaves=cassandra_obj.getPendingLeaves()
        lg.log('INFO', 'leave-manage page returned successfully')
        return render_template('leave-manage.html', pending_leaves=pending_leaves)
    except Exception as e:
        flash('Something went wrong', 'danger')
        lg.log('ERROR', f'Failed to return leave-manage page : {e}')
        return redirect(url_for('admin'))

@app.route('/<emp_id>/approveLeave', methods=['POST', 'GET'])
@cross_origin()
@login_required
def approve_leave(emp_id):
    try:
        emp_id=emp_id
        cassandra_obj.approveLeave(emp_id)
        flash(f'Leave approved for Emp ID:{emp_id}', 'success')
        lg.log('INFO', 'Leave approved successfully')
        return redirect(url_for('appliedLeaves'))
    except Exception as e:
        flash('Something went wrong', 'danger')
        lg.log('ERROR', f'Failed to approve leave : {e}')
        return redirect(url_for('appliedLeaves'))

@app.route('/<emp_id>/rejectLeave', methods=['POST', 'GET'])
@cross_origin()
@login_required
def reject_leave(emp_id):
    try:
        emp_id=emp_id
        cassandra_obj.rejectLeave(emp_id)
        flash(f'Leave rejected for Emp ID:{emp_id}', 'warning')
        lg.log('INFO', 'Leave rejected successfully')
        return redirect(url_for('appliedLeaves'))
    except Exception as e:
        flash('Something went wrong', 'danger')
        lg.log('ERROR', f'Failed to reject leave : {e}')
        return redirect(url_for('appliedLeaves'))

@app.route('/leave-status', methods=['POST', 'GET'])
@cross_origin()
@login_required
def leave_status():
    try:
        email_id=session.get('email_id')
        info=cassandra_obj.get_leave_status(email_id)
        if info== None:
            message='No leave found'
            flash(message, 'warning')
            lg.log('INFO', 'Leave status shown successfully')
            return redirect(url_for('employee'))
        else:
            leave_status, leave_date, leave_type=info[0], info[1], info[2]
            if leave_status=='Approved':
                message=f"Your leave has been approved for {leave_date} and for {leave_type} day"
                flash(message, 'success')
                lg.log('INFO', 'Leave status shown successfully')
                return redirect(url_for('employee'))
            elif leave_status=='Rejected':
                message=f"Your leave for {leave_date} has been rejected"
                flash(message, 'warning')
                lg.log('INFO', 'Leave status shown successfully')
                return redirect(url_for('employee'))
            elif leave_status=='Pending':
                message=f"Your leave for {leave_date} still pending"
                flash(message, 'primary')
                lg.log('INFO', 'Leave status shown successfully')
                return redirect(url_for('employee'))
    except Exception as e:
        flash('Something went wrong', 'danger')
        lg.log('ERROR', f'Failed to show leave status : {e}')
        return redirect(url_for('employee'))


@app.route('/claim-money', methods=['POST', 'GET'])
@cross_origin()
@login_required
def claim_money():
    try:
        email_id=session.get('email_id')
        money=cassandra_obj.get_money(email_id)
        if money==None:
            message='No money available to claim'
            flash(message,'warning')
            lg.log('INFO', 'Shown availabe money successfully')
            return redirect(url_for('employee'))
        else:
            message=f"{money} INR has been credited to your account successfully"
            flash(message, 'success')
            lg.log('INFO', 'Shown availabe money successfully')
            return redirect(url_for('employee'))
    except Exception as e:
        flash('Something went wrong', 'danger')
        lg.log('ERROR', f'Failed to show available money : {e}')
        return redirect(url_for('employee'))


if __name__ == "__main__":
    app.run()
