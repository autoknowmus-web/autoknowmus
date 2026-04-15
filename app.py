{% extends "layout.html" %}
{% block content %}
<div class="row justify-content-center text-center">
    <div class="col-lg-10">
        <div class="card border-0 shadow-lg p-5" style="background-color: #16181d; border-radius: 24px;">
            <h1 class="display-5 fw-bold mb-5 text-white">Welcome {{ user_name }}, what is your goal today?</h1>
            
            <div class="row g-4 justify-content-center">
                <div class="col-md-5">
                    <div class="mb-4">
                        <i class="fas fa-car fa-5x text-green"></i>
                    </div>
                    <a href="/seller" class="text-decoration-none">
                        <button class="btn btn-success w-100 py-4 fw-bold fs-4 mb-3">I am a Seller</button>
                    </a>
                    <p class="text-white-50">Get an instant valuation for the car you want to sell.</p>
                </div>

                <div class="col-md-5">
                    <div class="mb-4">
                        <i class="fas fa-search-dollar fa-5x text-blue"></i>
                    </div>
                    <a href="/buyer" class="text-decoration-none">
                        <button class="btn btn-primary w-100 py-4 fw-bold fs-4 mb-3" style="background-color: #4e54ff; border: none;">I am a Buyer</button>
                    </a>
                    <p class="text-white-50">Find fair market prices and 5-year forecasts for your next car.</p>
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}
